package graphdb

import (
	"bytes"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// DB is the main graph database instance. It manages one or more shards,
// provides concurrent query execution, and exposes all graph operations.
//
// Concurrency model:
//   - All read operations (GetNode, BFS, DFS, Query, etc.) can run fully in parallel.
//   - Write operations (AddNode, AddEdge, etc.) are serialized per-shard by bbolt.
//   - The closed flag is an atomic.Bool so reads never hold a mutex — no recursive lock issues.
type DB struct {
	opts             Options
	dir              string
	shards           []*shard
	pool             *workerPool
	cache            queryCache     // Cypher AST cache — avoids re-parsing identical queries
	ncache           *nodeCache     // LRU hot-node cache — avoids repeated bbolt lookups
	log              *slog.Logger   // structured logger for all operations
	mu               sync.Mutex     // only used in Close() to prevent double-close
	closed           atomic.Bool    // atomic flag — checked by every operation without locking
	indexedProps     sync.Map       // map[string]bool — tracks which property names have secondary indexes
	compositeIndexes sync.Map       // map[string]compositeIndexDef — tracks composite indexes
	metrics          *Metrics       // operational counters (Prometheus-compatible)
	slowLog          *slowQueryLog  // ring buffer of recent slow queries
	governor         *queryGovernor // enforces per-query resource limits (row cap, default timeout)
	compactQuit      chan struct{}  // closed in Close() to stop background compaction goroutine
	wal              *WAL           // write-ahead log for replication (nil if disabled)
}

// Open creates or opens a graph database at the given directory path.
// The directory will be created if it doesn't exist.
func Open(dir string, opts Options) (*DB, error) {
	if opts.ShardCount <= 0 {
		opts.ShardCount = 1
	}
	if opts.WorkerPoolSize <= 0 {
		opts.WorkerPoolSize = 8
	}
	if opts.CacheBudget <= 0 {
		opts.CacheBudget = 128 * 1024 * 1024 // 128MB
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	db := &DB{
		opts:   opts,
		dir:    dir,
		shards: make([]*shard, opts.ShardCount),
		cache:  newQueryCache(defaultQueryCacheCapacity),
		ncache: newNodeCache(opts.CacheBudget),
		log:    logger,
	}

	// Open all shards.
	for i := 0; i < opts.ShardCount; i++ {
		path := filepath.Join(dir, fmt.Sprintf("shard_%04d.db", i))
		s, err := openShard(path, opts)
		if err != nil {
			// Close any already-opened shards on failure.
			for j := 0; j < i; j++ {
				db.shards[j].close()
			}
			return nil, err
		}
		db.shards[i] = s
	}

	// Start the worker pool for concurrent queries.
	db.pool = newWorkerPool(opts.WorkerPoolSize)

	// Discover existing property indexes from disk (survives restart).
	db.discoverIndexes()
	db.discoverCompositeIndexes()

	db.metrics = newMetrics(db)
	db.slowLog = newSlowQueryLog(100)
	db.governor = &queryGovernor{
		maxRows:        opts.MaxResultRows,
		defaultTimeout: opts.DefaultQueryTimeout,
	}

	// Open WAL if replication is enabled.
	// The WAL records all committed mutations for follower replay.
	if opts.EnableWAL {
		wal, err := OpenWAL(dir, opts.WALNoSync, logger)
		if err != nil {
			for _, s := range db.shards {
				if s != nil {
					s.close()
				}
			}
			return nil, fmt.Errorf("graphdb: failed to open WAL: %w", err)
		}
		db.wal = wal
		db.log.Info("WAL enabled", "next_lsn", wal.nextLSN.Load())
	}

	// Start background compaction if configured.
	// The goroutine periodically rewrites shard files to reclaim disk space
	// from deleted B+ tree pages. Stopped when compactQuit is closed in Close().
	db.compactQuit = make(chan struct{})
	if opts.CompactionInterval > 0 {
		db.startCompactionLoop(opts.CompactionInterval, db.compactQuit)
		db.log.Info("background compaction enabled", "interval", opts.CompactionInterval)
	}

	db.log.Info("database opened",
		"dir", dir,
		"shards", opts.ShardCount,
		"workers", opts.WorkerPoolSize,
		"cache_budget_bytes", opts.CacheBudget,
	)

	return db, nil
}

// discoverIndexes scans the idx_prop bucket to learn which properties are indexed.
// Uses seek-jumping so cost is O(#unique indexed properties), not O(#index entries).
func (db *DB) discoverIndexes() {
	for _, s := range db.shards {
		_ = s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketIdxProp)
			if b == nil {
				return nil
			}
			c := b.Cursor()
			k, _ := c.First()
			for k != nil {
				colonIdx := bytes.IndexByte(k, ':')
				if colonIdx > 0 {
					prop := string(k[:colonIdx])
					db.indexedProps.Store(prop, true)
					// Jump past all entries for this property.
					nextPrefix := make([]byte, colonIdx+1)
					copy(nextPrefix, k[:colonIdx])
					nextPrefix[colonIdx] = ':' + 1 // ';' > ':'
					k, _ = c.Seek(nextPrefix)
				} else {
					k, _ = c.Next()
				}
			}
			return nil
		})
	}
}

// HasIndex returns true if a secondary index exists for the given property name.
func (db *DB) HasIndex(propName string) bool {
	_, ok := db.indexedProps.Load(propName)
	return ok
}

// Close gracefully shuts down the database, flushing all pending writes.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return nil
	}
	db.closed.Store(true)

	// Stop background compaction goroutine (if running).
	if db.compactQuit != nil {
		close(db.compactQuit)
	}

	// Stop worker pool.
	if db.pool != nil {
		db.pool.stop()
	}

	// Close WAL (flush final entries).
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			db.log.Error("WAL close error", "error", err)
		}
	}

	// Close all shards.
	var firstErr error
	for _, s := range db.shards {
		if s != nil {
			if err := s.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		db.log.Error("database closed with error", "error", firstErr)
	} else {
		db.log.Info("database closed")
	}

	return firstErr
}

// ErrReadOnlyReplica is returned when a write operation is attempted on a
// follower node. Followers only accept writes from the internal WAL applier.
var ErrReadOnlyReplica = fmt.Errorf("graphdb: this is a read-only replica")

// isFollower returns true if this DB instance is configured as a follower.
// Thread-safe — reads are protected by db.mu since SetRole can change the role.
func (db *DB) isFollower() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.opts.Role == "follower"
}

// writeGuard returns ErrReadOnlyReplica if the DB is a follower.
// Every public write method should call this before proceeding.
func (db *DB) writeGuard() error {
	if db.isFollower() {
		return ErrReadOnlyReplica
	}
	return nil
}

// walAppend is a convenience helper that appends a WAL entry if the WAL is
// enabled. It encodes the payload, appends the entry, and logs any errors.
// Called after a successful bbolt commit — the WAL contains only committed ops.
// Returns nil if the WAL is disabled or the append succeeds.
func (db *DB) walAppend(op OpType, payloadStruct any) {
	if db.wal == nil {
		return
	}
	payload, err := encodeWALPayload(payloadStruct)
	if err != nil {
		db.log.Error("WAL: failed to encode payload", "op", op, "error", err)
		return
	}
	if _, err := db.wal.Append(op, payload); err != nil {
		db.log.Error("WAL: append failed — mutation committed but not logged",
			"op", op, "error", err)
	}
}

// SetRole dynamically changes the node's role at runtime.
// This is called by the Raft election callback when leadership changes.
// Switching to "follower" causes all subsequent public writes to be rejected.
// Switching to "leader" re-enables writes.
func (db *DB) SetRole(role string) {
	db.mu.Lock()
	old := db.opts.Role
	db.opts.Role = role
	db.mu.Unlock()
	db.log.Info("role changed", "old", old, "new", role)
}

// Role returns the current role of this DB instance.
func (db *DB) Role() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.opts.Role
}

// WAL returns the write-ahead log, or nil if WAL is not enabled.
// Used by the replication package to create readers for log shipping.
func (db *DB) WAL() *WAL {
	return db.wal
}

// Metrics returns the operational metrics collector.
// Use this to read counters or write Prometheus exposition format.
func (db *DB) Metrics() *Metrics {
	return db.metrics
}

// isClosed is a cheap inline check used by every public method.
func (db *DB) isClosed() bool {
	return db.closed.Load()
}

// shardFor returns the shard responsible for the given node ID.
// Uses consistent hash-based partitioning.
func (db *DB) shardFor(id NodeID) *shard {
	if len(db.shards) == 1 {
		return db.shards[0]
	}
	return db.shards[uint64(id)%uint64(len(db.shards))]
}

// shardForEdge returns the shard responsible for the given edge.
// Edges are co-located with their source node for efficient outgoing traversals.
func (db *DB) shardForEdge(fromID NodeID) *shard {
	return db.shardFor(fromID)
}

// primaryShard returns the first shard (used for single-shard operations).
func (db *DB) primaryShard() *shard {
	return db.shards[0]
}

// Stats returns aggregate statistics across all shards.
func (db *DB) Stats() (*GraphStats, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	stats := &GraphStats{
		ShardCount: len(db.shards),
	}

	for _, s := range db.shards {
		stats.NodeCount += s.nodeCount.Load()
		stats.EdgeCount += s.edgeCount.Load()
		size, err := s.fileSize()
		if err != nil {
			return nil, err
		}
		stats.DiskSizeBytes += size
	}

	return stats, nil
}
