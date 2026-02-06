package graphdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// Bucket names used in bbolt.
var (
	bucketMeta       = []byte("meta")
	bucketNodes      = []byte("nodes")
	bucketEdges      = []byte("edges")
	bucketAdjOut     = []byte("adj_out")
	bucketAdjIn      = []byte("adj_in")
	bucketIdxLabel   = []byte("idx_label")     // node property "label" index
	bucketIdxEdgeTyp = []byte("idx_edge_type") // edge label/type index
	bucketIdxProp    = []byte("idx_prop")      // generic property index

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
}

// shard represents a single bbolt database file (a partition of the graph).
type shard struct {
	db   *bolt.DB
	path string

	// Atomic counters cached in memory for fast ID generation.
	nextNodeID atomic.Uint64
	nextEdgeID atomic.Uint64
	nodeCount  atomic.Uint64
	edgeCount  atomic.Uint64
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

	s := &shard{db: db, path: path}

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
func (s *shard) loadCounters() error {
	return s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if meta == nil {
			return nil
		}

		if v := meta.Get(metaNextNodeID); v != nil {
			s.nextNodeID.Store(decodeUint64(v))
		}
		if v := meta.Get(metaNextEdgeID); v != nil {
			s.nextEdgeID.Store(decodeUint64(v))
		}
		if v := meta.Get(metaNodeCount); v != nil {
			s.nodeCount.Store(decodeUint64(v))
		}
		if v := meta.Get(metaEdgeCount); v != nil {
			s.edgeCount.Store(decodeUint64(v))
		}
		return nil
	})
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

// close closes the bbolt database.
func (s *shard) close() error {
	// Persist final counter state.
	if !s.db.IsReadOnly() {
		_ = s.db.Update(func(tx *bolt.Tx) error {
			return s.persistCounters(tx)
		})
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
