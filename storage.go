package graphdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrWriteQueueFull is returned when the write semaphore is full and the
// caller's context expires before a slot becomes available. This signals
// that the database is under heavy write pressure and the caller should
// retry or apply application-level backpressure.
var ErrWriteQueueFull = errors.New("graphdb: write queue full")

// Bucket names used in bbolt.
var (
	bucketMeta         = []byte("meta")
	bucketNodes        = []byte("nodes")
	bucketEdges        = []byte("edges")
	bucketAdjOut       = []byte("adj_out")
	bucketAdjIn        = []byte("adj_in")
	bucketIdxLabel     = []byte("idx_label")      // node property "label" index (legacy)
	bucketIdxEdgeTyp   = []byte("idx_edge_type")  // edge label/type index
	bucketIdxProp      = []byte("idx_prop")       // generic property index
	bucketNodeLabels   = []byte("node_labels")    // nodeID → msgpack []string (per-node labels)
	bucketIdxNodeLabel = []byte("idx_node_label") // "Label\x00" + nodeID → nil (label→node index)
	bucketIdxComposite = []byte("idx_composite")  // composite property index
	bucketIdxUnique    = []byte("idx_unique")     // unique constraint value index
	bucketUniqueMeta   = []byte("unique_meta")    // unique constraint metadata

	// Meta keys
	metaNextNodeID = []byte("next_node_id")
	metaNextEdgeID = []byte("next_edge_id")
	metaNodeCount  = []byte("node_count")
	metaEdgeCount  = []byte("edge_count")
)

// allBuckets is the list of all buckets to create on initialization.
var allBuckets = [][]byte{
	bucketMeta,
	bucketNodes,
	bucketEdges,
	bucketAdjOut,
	bucketAdjIn,
	bucketIdxLabel,
	bucketIdxEdgeTyp,
	bucketIdxProp,
	bucketNodeLabels,
	bucketIdxNodeLabel,
	bucketIdxComposite,
	bucketIdxUnique,
	bucketUniqueMeta,
}

// shardSyncInterval is how often the background goroutine calls db.Sync()
// when NoSync is enabled. At most this much committed data may be lost on
// an unclean shutdown (power failure, SIGKILL). 200ms is a good balance
// between durability and throughput — comparable to MongoDB's journal interval.
const shardSyncInterval = 200 * time.Millisecond

// shard represents a single bbolt database file (a partition of the graph).
type shard struct {
	db   *bolt.DB
	path string

	// Atomic counters cached in memory for fast ID generation.
	nextNodeID atomic.Uint64
	nextEdgeID atomic.Uint64
	nodeCount  atomic.Uint64
	edgeCount  atomic.Uint64

	// writeSem is a bounded semaphore that limits the number of goroutines
	// waiting to acquire bbolt's single-writer lock. Without this, under
	// heavy write load, goroutines pile up unbounded on bbolt's internal
	// mutex, consuming memory and goroutine stacks. The semaphore provides
	// backpressure: callers block on the channel (respecting context) and
	// get ErrWriteQueueFull if the queue is full and their deadline expires.
	writeSem     chan struct{}
	writeTimeout time.Duration // max wait time for a write slot; 0 = block forever

	// Background sync for NoSync mode. When per-tx fdatasync is disabled,
	// a goroutine periodically calls db.Sync() to flush dirty pages to disk.
	stopSync chan struct{} // closed to signal background sync to stop (nil if NoSync=false)
	syncDone chan struct{} // closed when background sync goroutine exits
}

// openShard opens or creates a bbolt database file at the given path.
func openShard(path string, opts Options) (*shard, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("graphdb: failed to create directory %s: %w", dir, err)
	}

	boltOpts := bolt.DefaultOptions
	boltOpts.NoSync = opts.NoSync
	boltOpts.ReadOnly = opts.ReadOnly
	if opts.MmapSize > 0 {
		boltOpts.InitialMmapSize = opts.MmapSize
	}

	db, err := bolt.Open(path, 0600, boltOpts)
	if err != nil {
		return nil, fmt.Errorf("graphdb: failed to open bolt db at %s: %w", path, err)
	}

	// When NoSync is enabled, per-transaction fdatasync is disabled.
	// Set MaxBatchDelay=0 so Batch() fires immediately instead of waiting
	// the default 10ms to collect concurrent callers. Natural batching
	// still occurs for truly concurrent writers arriving in the same ~µs
	// window. Without this, each write pays a 10ms artificial delay even
	// though there's no fsync to amortize.
	if opts.NoSync {
		db.MaxBatchDelay = 0
	}

	// Default write queue: 64 slots. This allows up to 64 goroutines to
	// queue for bbolt's single-writer lock. Beyond that, callers block on
	// the channel and may time out.
	queueSize := opts.WriteQueueSize
	if queueSize <= 0 {
		queueSize = 64
	}

	s := &shard{
		db:           db,
		path:         path,
		writeSem:     make(chan struct{}, queueSize),
		writeTimeout: opts.WriteTimeout,
	}

	// Initialize buckets and load counters.
	if !opts.ReadOnly {
		if err := s.initBuckets(); err != nil {
			db.Close()
			return nil, err
		}
	}

	if err := s.loadCounters(); err != nil {
		db.Close()
		return nil, err
	}

	// Start background sync goroutine for NoSync mode.
	// This is the ONLY place where fdatasync happens when NoSync=true.
	if opts.NoSync && !opts.ReadOnly {
		s.stopSync = make(chan struct{})
		s.syncDone = make(chan struct{})
		go s.backgroundSync()
	}

	return s, nil
}

// initBuckets creates all required buckets if they don't exist.
func (s *shard) initBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range allBuckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("graphdb: failed to create bucket %s: %w", name, err)
			}
		}

		// Initialize meta counters if not present.
		meta := tx.Bucket(bucketMeta)
		for _, key := range [][]byte{metaNextNodeID, metaNextEdgeID, metaNodeCount, metaEdgeCount} {
			if meta.Get(key) == nil {
				if err := meta.Put(key, encodeUint64(0)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// loadCounters loads atomic counters from the meta bucket into memory.
// If the persisted counters look stale (e.g. the previous process exited
// without calling Close), we recover them from the actual data in the
// nodes/edges buckets so that IDs don't collide and counts are accurate.
func (s *shard) loadCounters() error {
	return s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if meta == nil {
			return nil
		}

		// Load persisted values.
		var nextNode, nextEdge, nCount, eCount uint64
		if v := meta.Get(metaNextNodeID); v != nil {
			nextNode = decodeUint64(v)
		}
		if v := meta.Get(metaNextEdgeID); v != nil {
			nextEdge = decodeUint64(v)
		}
		if v := meta.Get(metaNodeCount); v != nil {
			nCount = decodeUint64(v)
		}
		if v := meta.Get(metaEdgeCount); v != nil {
			eCount = decodeUint64(v)
		}

		// ── Self-heal: recover from actual bucket data if meta is stale ──
		// This handles the case where a previous process exited without
		// calling db.Close() (so persistCounters was never invoked).
		if nb := tx.Bucket(bucketNodes); nb != nil {
			actualCount := uint64(nb.Stats().KeyN)
			if actualCount > nCount {
				nCount = actualCount
			}
			// Find highest node ID (last key, since keys are big-endian uint64).
			if k, _ := nb.Cursor().Last(); k != nil {
				maxID := decodeUint64(k)
				if maxID >= nextNode {
					nextNode = maxID + 1
				}
			}
		}
		if eb := tx.Bucket(bucketEdges); eb != nil {
			actualCount := uint64(eb.Stats().KeyN)
			if actualCount > eCount {
				eCount = actualCount
			}
			if k, _ := eb.Cursor().Last(); k != nil {
				maxID := decodeUint64(k)
				if maxID >= nextEdge {
					nextEdge = maxID + 1
				}
			}
		}

		s.nextNodeID.Store(nextNode)
		s.nextEdgeID.Store(nextEdge)
		s.nodeCount.Store(nCount)
		s.edgeCount.Store(eCount)
		return nil
	})
}

// ---------------------------------------------------------------------------
// Write backpressure — bounded semaphore protecting bbolt's single-writer lock.
// ---------------------------------------------------------------------------

// acquireWrite obtains a slot in the write semaphore, blocking until a slot
// is available, the context is cancelled, or the write timeout expires.
//
// The flow is:
//  1. Try to send on writeSem (buffered channel) — succeeds immediately if
//     the queue has capacity.
//  2. If the channel is full, block until a slot opens or the deadline fires.
//  3. On timeout/cancellation, return ErrWriteQueueFull.
//
// The caller MUST call releaseWrite() when the write operation completes.
func (s *shard) acquireWrite(ctx context.Context) error {
	// If a write timeout is configured and the caller has no deadline,
	// apply it so writes don't block indefinitely under contention.
	if s.writeTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.writeTimeout)
			defer cancel()
		}
	}

	select {
	case s.writeSem <- struct{}{}:
		return nil // acquired
	default:
		// Channel full — wait with context.
		select {
		case s.writeSem <- struct{}{}:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrWriteQueueFull, ctx.Err())
		}
	}
}

// releaseWrite returns a slot to the write semaphore. Must be called exactly
// once after a successful acquireWrite.
func (s *shard) releaseWrite() {
	<-s.writeSem
}

// writeUpdate is a convenience wrapper that acquires the write semaphore,
// executes a bbolt Batch transaction, and releases the semaphore.
//
// With NoSync=true (default), MaxBatchDelay=0 so Batch() fires immediately.
// Concurrent callers that arrive in the same microsecond window are still
// naturally grouped into a single transaction. Without NoSync, the default
// MaxBatchDelay=10ms groups writers to amortize fdatasync across the batch.
//
// Caveat: if any function in a batch fails, the entire batch is rolled
// back and each function is retried individually via Update(). This
// means fn may be called more than once — all operations inside fn
// must be idempotent (bbolt Put/Delete are naturally idempotent).
// Counter increments (nodeCount, edgeCount) should be done AFTER
// writeUpdate returns successfully, not inside fn.
func (s *shard) writeUpdate(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	return s.db.Batch(fn)
}

// writeUpdateSingle is like writeUpdate but uses a dedicated bbolt
// Update() transaction that is NOT batched. Use this for operations
// that must run in isolation (e.g. bucket creation, compaction).
func (s *shard) writeUpdateSingle(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	return s.db.Update(fn)
}

// allocNodeID atomically allocates a new node ID.
func (s *shard) allocNodeID() NodeID {
	return NodeID(s.nextNodeID.Add(1))
}

// allocEdgeID atomically allocates a new edge ID.
func (s *shard) allocEdgeID() EdgeID {
	return EdgeID(s.nextEdgeID.Add(1))
}

// persistCounters writes the current atomic counters to the meta bucket.
// Must be called within a write transaction.
func (s *shard) persistCounters(tx *bolt.Tx) error {
	meta := tx.Bucket(bucketMeta)
	if err := meta.Put(metaNextNodeID, encodeUint64(s.nextNodeID.Load())); err != nil {
		return err
	}
	if err := meta.Put(metaNextEdgeID, encodeUint64(s.nextEdgeID.Load())); err != nil {
		return err
	}
	if err := meta.Put(metaNodeCount, encodeUint64(s.nodeCount.Load())); err != nil {
		return err
	}
	if err := meta.Put(metaEdgeCount, encodeUint64(s.edgeCount.Load())); err != nil {
		return err
	}
	return nil
}

// backgroundSync periodically calls db.Sync() to flush dirty pages to disk.
// Only runs when NoSync=true (per-tx fdatasync disabled). The goroutine exits
// when stopSync is closed, performing one final sync before returning.
func (s *shard) backgroundSync() {
	defer close(s.syncDone)
	ticker := time.NewTicker(shardSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopSync:
			// Final sync before exit — ensures all committed data reaches disk.
			_ = s.db.Sync()
			return
		case <-ticker.C:
			_ = s.db.Sync()
		}
	}
}

// close closes the bbolt database.
func (s *shard) close() error {
	// Stop background sync goroutine first (if running).
	// This triggers a final db.Sync() inside the goroutine.
	if s.stopSync != nil {
		close(s.stopSync)
		<-s.syncDone // wait for final sync to complete
	}

	// Persist final counter state.
	if !s.db.IsReadOnly() {
		_ = s.db.Update(func(tx *bolt.Tx) error {
			return s.persistCounters(tx)
		})
		// Sync the counter update to disk (background sync is already stopped).
		_ = s.db.Sync()
	}
	return s.db.Close()
}

// fileSize returns the size of the database file in bytes.
func (s *shard) fileSize() (int64, error) {
	info, err := os.Stat(s.path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// forEachInBucket iterates over all key-value pairs in a bucket using a read transaction.
func (s *shard) forEachInBucket(bucketName []byte, fn func(k, v []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return nil
		}
		return b.ForEach(fn)
	})
}

// forEachWithPrefix iterates over keys with a given prefix in a bucket.
func (s *shard) forEachWithPrefix(bucketName, prefix []byte, fn func(k, v []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}
