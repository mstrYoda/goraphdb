package graphdb

import (
	"context"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// AddNode creates a new node with the given arbitrary properties.
// Returns the auto-generated NodeID. Safe for concurrent use.
func (db *DB) AddNode(props Props) (NodeID, error) {
	if db.isClosed() {
		return 0, fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return 0, err
	}

	// For sharded mode, allocate ID from primary shard to ensure global uniqueness.
	s := db.primaryShard()
	id := s.allocNodeID()

	// Determine which shard owns this node.
	target := db.shardFor(id)

	data, err := encodeProps(props)
	if err != nil {
		return 0, fmt.Errorf("graphdb: failed to encode node properties: %w", err)
	}

	// writeUpdate acquires the write semaphore before entering bbolt's
	// single-writer lock, providing bounded backpressure under load.
	err = target.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		if err := b.Put(encodeNodeID(id), data); err != nil {
			return err
		}
		// Maintain secondary indexes (same tx, no extra fsync).
		if err := db.indexNodeProps(tx, id, props); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.log.Error("failed to add node", "error", err)
		return 0, fmt.Errorf("graphdb: failed to add node: %w", err)
	}
	// Increment counter after successful commit (outside batch fn to avoid
	// double-counting if bbolt Batch retries the function on rollback).
	target.nodeCount.Add(1)

	// WAL: log the committed mutation for replication.
	db.walAppend(OpAddNode, WALAddNode{ID: id, Props: props})

	db.ncache.Put(&Node{ID: id, Props: props})
	if db.metrics != nil {
		db.metrics.NodesCreated.Add(1)
	}
	db.log.Debug("node added", "id", id)
	return id, nil
}

// AddNodeBatch creates multiple nodes in a single transaction for performance.
// Returns the list of auto-generated NodeIDs. Safe for concurrent use.
//
// Note: AddNodeBatch does NOT auto-maintain secondary indexes for performance.
// Large batches with inline index maintenance cause excessive B+tree page splits,
// degrading insert throughput by orders of magnitude. Call ReIndex() or
// CreateIndex() after a batch insert to rebuild indexes.
//
// Single-node AddNode, UpdateNode, SetNodeProps, and DeleteNode DO auto-maintain
// indexes since the overhead is negligible for individual operations.
func (db *DB) AddNodeBatch(propsList []Props) ([]NodeID, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return nil, err
	}

	ids := make([]NodeID, len(propsList))

	if len(db.shards) == 1 {
		// Single-shard fast path: all nodes go into one transaction.
		s := db.shards[0]
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketNodes)
			for i, props := range propsList {
				id := s.allocNodeID()
				ids[i] = id

				data, err := encodeProps(props)
				if err != nil {
					return err
				}
				if err := b.Put(encodeNodeID(id), data); err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil {
			s.nodeCount.Add(uint64(len(propsList)))
		}
		if err != nil {
			db.log.Error("batch add failed", "count", len(propsList), "error", err)
			return nil, fmt.Errorf("graphdb: batch add failed: %w", err)
		}
		// WAL: log the batch as a single entry.
		nodes := make([]WALBatchNode, len(propsList))
		for i, p := range propsList {
			nodes[i] = WALBatchNode{ID: ids[i], Props: p}
		}
		db.walAppend(OpAddNodeBatch, WALAddNodeBatch{Nodes: nodes})

		db.log.Debug("batch nodes added", "count", len(propsList))
		return ids, nil
	}

	// Multi-shard path: group nodes by target shard.
	type shardEntry struct {
		index int
		id    NodeID
		data  []byte
	}
	shardGroups := make(map[int][]shardEntry)

	s := db.primaryShard()
	for i, props := range propsList {
		id := s.allocNodeID()
		ids[i] = id

		data, err := encodeProps(props)
		if err != nil {
			return nil, fmt.Errorf("graphdb: failed to encode props at index %d: %w", i, err)
		}

		shardIdx := int(uint64(id) % uint64(len(db.shards)))
		shardGroups[shardIdx] = append(shardGroups[shardIdx], shardEntry{
			index: i, id: id, data: data,
		})
	}

	// Write each shard's batch in a separate transaction.
	for shardIdx, entries := range shardGroups {
		target := db.shards[shardIdx]
		err := target.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketNodes)
			for _, e := range entries {
				if err := b.Put(encodeNodeID(e.id), e.data); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("graphdb: batch add failed on shard %d: %w", shardIdx, err)
		}
		target.nodeCount.Add(uint64(len(entries)))
	}

	// WAL: log the batch as a single entry.
	nodes := make([]WALBatchNode, len(propsList))
	for i, p := range propsList {
		nodes[i] = WALBatchNode{ID: ids[i], Props: p}
	}
	db.walAppend(OpAddNodeBatch, WALAddNodeBatch{Nodes: nodes})

	return ids, nil
}

// GetNode retrieves a node by its ID. Safe for concurrent use.
func (db *DB) GetNode(id NodeID) (*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return db.getNode(id)
}

// getNode is the lock-free internal version of GetNode.
// Called by BFS/DFS/ShortestPath etc. which need to call this many times during traversal.
// Uses the hot-node LRU cache to avoid repeated bbolt lookups.
func (db *DB) getNode(id NodeID) (*Node, error) {
	// Fast path: cache hit.
	if n := db.ncache.Get(id); n != nil {
		return n, nil
	}

	s := db.shardFor(id)
	var node *Node

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		data := b.Get(encodeNodeID(id))
		if data == nil {
			return fmt.Errorf("graphdb: node %d not found", id)
		}

		props, err := decodeProps(data)
		if err != nil {
			return err
		}

		labels := loadLabels(tx, id)
		node = &Node{ID: id, Labels: labels, Props: props}
		return nil
	})

	// Populate cache on successful read.
	if err == nil && node != nil {
		db.ncache.Put(node)
	}

	return node, err
}

// UpdateNode updates the properties of an existing node.
// The update is a merge: existing properties are kept unless overwritten.
// Secondary indexes are updated automatically for changed indexed properties.
func (db *DB) UpdateNode(id NodeID, props Props) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	s := db.shardFor(id)
	err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		key := encodeNodeID(id)

		existing := b.Get(key)
		if existing == nil {
			return fmt.Errorf("graphdb: node %d not found", id)
		}

		// Decode old properties for index maintenance.
		oldProps, err := decodeProps(existing)
		if err != nil {
			return err
		}

		// Remove old index entries before mutation.
		if err := db.unindexNodeProps(tx, id, oldProps); err != nil {
			return err
		}

		// Merge properties.
		for k, v := range props {
			oldProps[k] = v
		}

		// Enforce unique constraints on the merged properties.
		labels := loadLabels(tx, id)
		if len(labels) > 0 {
			// Unindex old unique values, check new ones, then re-index.
			_ = db.unindexUniqueConstraints(tx, id, labels, oldProps)
			if err := db.checkUniqueConstraintsInTx(tx, labels, oldProps, id); err != nil {
				return err
			}
		}

		data, err := encodeProps(oldProps)
		if err != nil {
			return err
		}
		if err := b.Put(key, data); err != nil {
			return err
		}

		// Add new index entries with merged properties.
		if err := db.indexNodeProps(tx, id, oldProps); err != nil {
			return err
		}
		// Re-index unique constraints with new values.
		if len(labels) > 0 {
			_ = db.indexUniqueConstraints(tx, id, labels, oldProps)
		}
		return nil
	})
	if err != nil {
		db.log.Error("failed to update node", "id", id, "error", err)
	} else {
		db.walAppend(OpUpdateNode, WALUpdateNode{ID: id, Props: props})
		db.ncache.Invalidate(id)
		db.log.Debug("node updated", "id", id)
	}
	return err
}

// SetNodeProps replaces all properties of a node (full overwrite).
// Secondary indexes are updated automatically.
func (db *DB) SetNodeProps(id NodeID, props Props) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	s := db.shardFor(id)
	err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		key := encodeNodeID(id)

		existing := b.Get(key)
		if existing == nil {
			return fmt.Errorf("graphdb: node %d not found", id)
		}

		// Remove old index entries.
		oldProps, err := decodeProps(existing)
		if err != nil {
			return err
		}
		if err := db.unindexNodeProps(tx, id, oldProps); err != nil {
			return err
		}

		// Enforce unique constraints on the new properties.
		labels := loadLabels(tx, id)
		if len(labels) > 0 {
			_ = db.unindexUniqueConstraints(tx, id, labels, oldProps)
			if err := db.checkUniqueConstraintsInTx(tx, labels, props, id); err != nil {
				return err
			}
		}

		data, err := encodeProps(props)
		if err != nil {
			return err
		}
		if err := b.Put(key, data); err != nil {
			return err
		}

		// Add new index entries.
		if err := db.indexNodeProps(tx, id, props); err != nil {
			return err
		}
		if len(labels) > 0 {
			_ = db.indexUniqueConstraints(tx, id, labels, props)
		}
		return nil
	})
	if err != nil {
		db.log.Error("failed to set node props", "id", id, "error", err)
	} else {
		db.walAppend(OpSetNodeProps, WALSetNodeProps{ID: id, Props: props})
		db.ncache.Invalidate(id)
		db.log.Debug("node props set", "id", id)
	}
	return err
}

// DeleteNode removes a node and all its associated edges.
func (db *DB) DeleteNode(id NodeID) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	// First, collect all edges connected to this node.
	edges, err := db.getEdgesForNode(id, Both)
	if err != nil {
		return err
	}

	// Delete all connected edges.
	for _, e := range edges {
		if err := db.deleteEdgeInternal(e); err != nil {
			return fmt.Errorf("graphdb: failed to delete edge %d while deleting node %d: %w", e.ID, id, err)
		}
	}

	// Delete the node itself (and clean up index entries).
	s := db.shardFor(id)
	err = s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		key := encodeNodeID(id)

		existing := b.Get(key)
		if existing == nil {
			return fmt.Errorf("graphdb: node %d not found", id)
		}

		// Remove property index entries before deleting the node.
		props, _ := decodeProps(existing)
		if len(props) > 0 {
			_ = db.unindexNodeProps(tx, id, props)
		}

		// Remove label index entries and unique constraint index.
		labels := loadLabels(tx, id)
		if len(labels) > 0 {
			// Clean up unique constraint index entries.
			if len(props) > 0 {
				_ = db.unindexUniqueConstraints(tx, id, labels, props)
			}
			idxBucket := tx.Bucket(bucketIdxNodeLabel)
			for _, l := range labels {
				_ = idxBucket.Delete(encodeLabelIndexKey(l, id))
			}
			_ = tx.Bucket(bucketNodeLabels).Delete(encodeNodeID(id))
		}

		if err := b.Delete(key); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.log.Error("failed to delete node", "id", id, "error", err)
	} else {
		s.nodeCount.Add(^uint64(0)) // decrement by 1
		db.walAppend(OpDeleteNode, WALDeleteNode{ID: id})
		db.ncache.Invalidate(id)
		if db.metrics != nil {
			db.metrics.NodesDeleted.Add(1)
		}
		db.log.Debug("node deleted", "id", id, "edges_removed", len(edges))
	}
	return err
}

// NodeExists checks if a node exists. Safe for concurrent use.
func (db *DB) NodeExists(id NodeID) (bool, error) {
	if db.isClosed() {
		return false, fmt.Errorf("graphdb: database is closed")
	}

	s := db.shardFor(id)
	exists := false
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		exists = b.Get(encodeNodeID(id)) != nil
		return nil
	})
	return exists, err
}

// NodeCount returns the total number of nodes in the database.
func (db *DB) NodeCount() uint64 {
	var total uint64
	for _, s := range db.shards {
		total += s.nodeCount.Load()
	}
	return total
}

// ForEachNode iterates over all nodes, calling fn for each.
// Return a non-nil error from fn to stop iteration. Safe for concurrent use.
func (db *DB) ForEachNode(fn func(*Node) error) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	return db.forEachNode(fn)
}

// forEachNode is the lock-free internal version.
func (db *DB) forEachNode(fn func(*Node) error) error {
	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketNodes)
			return b.ForEach(func(k, v []byte) error {
				id := decodeNodeID(k)
				props, err := decodeProps(v)
				if err != nil {
					return err
				}
				labels := loadLabels(tx, id)
				return fn(&Node{ID: id, Labels: labels, Props: props})
			})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// FindNodes returns all nodes matching the given filter. Safe for concurrent use.
func (db *DB) FindNodes(filter NodeFilter) ([]*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	var results []*Node
	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketNodes)
			return b.ForEach(func(k, v []byte) error {
				id := decodeNodeID(k)
				props, err := decodeProps(v)
				if err != nil {
					return err
				}
				node := &Node{ID: id, Props: props}
				if filter(node) {
					results = append(results, node)
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}
