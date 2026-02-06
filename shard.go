package graphdb

import (
	"fmt"
	"hash/fnv"
)

// ShardInfo provides information about a specific shard.
type ShardInfo struct {
	Index     int    `json:"index"`
	Path      string `json:"path"`
	NodeCount uint64 `json:"node_count"`
	EdgeCount uint64 `json:"edge_count"`
	SizeBytes int64  `json:"size_bytes"`
}

// ShardStats returns information about each shard in the database.
func (db *DB) ShardStats() ([]ShardInfo, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	infos := make([]ShardInfo, len(db.shards))
	for i, s := range db.shards {
		size, err := s.fileSize()
		if err != nil {
			return nil, err
		}
		infos[i] = ShardInfo{
			Index:     i,
			Path:      s.path,
			NodeCount: s.nodeCount.Load(),
			EdgeCount: s.edgeCount.Load(),
			SizeBytes: size,
		}
	}
	return infos, nil
}

// ShardKeyFunc is a function that maps a node to a shard index.
// The default implementation uses hash-based partitioning on NodeID.
type ShardKeyFunc func(id NodeID, shardCount int) int

// DefaultShardKey returns the default shard key function (hash of NodeID).
func DefaultShardKey() ShardKeyFunc {
	return func(id NodeID, shardCount int) int {
		return int(uint64(id) % uint64(shardCount))
	}
}

// PropertyShardKey returns a shard key function that shards based on a property value.
// This is useful when you want related nodes to be co-located.
func PropertyShardKey(propName string) ShardKeyFunc {
	return func(id NodeID, shardCount int) int {
		h := fnv.New64a()
		h.Write([]byte(fmt.Sprintf("%s:%d", propName, id)))
		return int(h.Sum64() % uint64(shardCount))
	}
}

// RebalanceShards is a placeholder for future shard rebalancing.
// In a production system, this would redistribute nodes across shards
// when the shard count changes.
func (db *DB) RebalanceShards() error {
	if len(db.shards) <= 1 {
		return nil // nothing to rebalance
	}
	return fmt.Errorf("graphdb: shard rebalancing is not yet implemented")
}

// IsSharded returns true if the database uses multiple shards.
func (db *DB) IsSharded() bool {
	return len(db.shards) > 1
}
