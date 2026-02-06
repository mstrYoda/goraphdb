package graphdb

import (
	"bytes"
	"fmt"
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
	opts         Options
	dir          string
	shards       []*shard
	pool         *workerPool
	mu           sync.Mutex  // only used in Close() to prevent double-close
	closed       atomic.Bool // atomic flag — checked by every operation without locking
	indexedProps sync.Map    // map[string]bool — tracks which property names have secondary indexes
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
	if opts.CacheSize <= 0 {
		opts.CacheSize = 100_000
	}

	db := &DB{
		opts:   opts,
		dir:    dir,
		shards: make([]*shard, opts.ShardCount),
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

	// Stop worker pool.
	if db.pool != nil {
		db.pool.stop()
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

	return firstErr
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
