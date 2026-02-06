package graphdb

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// CreateIndex creates a secondary index on a node property.
// This allows fast lookups of nodes by property value.
// Example: CreateIndex("name") enables fast FindByProperty("name", "Alice").
// Cypher queries automatically use available indexes for inline property filters.
func (db *DB) CreateIndex(propName string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}

	for _, s := range db.shards {
		err := s.db.Update(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			nodesBucket := tx.Bucket(bucketNodes)

			// Scan all nodes and index the specified property.
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil // skip corrupted entries
				}

				val, ok := props[propName]
				if !ok {
					return nil
				}

				// Create index key: "propName:value" + nodeID
				idxKeyStr := fmt.Sprintf("%s:%v", propName, val)
				nodeID := decodeNodeID(k)
				idxKey := encodeIndexKey(idxKeyStr, uint64(nodeID))

				return idxBucket.Put(idxKey, nil)
			})
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to create index on %s: %w", propName, err)
		}
	}

	// Register the index in memory for the query optimizer.
	db.indexedProps.Store(propName, true)
	return nil
}

// FindByProperty finds nodes where the given property equals the given value.
// Uses the secondary index if available, otherwise falls back to full scan.
func (db *DB) FindByProperty(propName string, value interface{}) ([]*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	_, hasIndex := db.indexedProps.Load(propName)

	idxKeyStr := fmt.Sprintf("%s:%v", propName, value)
	prefix := encodeIndexPrefix(idxKeyStr)

	var nodes []*Node

	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			nodesBucket := tx.Bucket(bucketNodes)

			if hasIndex {
				// Fast path: use the secondary index (prefix seek).
				c := idxBucket.Cursor()
				for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
					nodeIDBytes := k[len(prefix):]
					if len(nodeIDBytes) < 8 {
						continue
					}
					nodeID := decodeNodeID(nodeIDBytes)

					data := nodesBucket.Get(encodeNodeID(nodeID))
					if data == nil {
						continue
					}
					props, err := decodeProps(data)
					if err != nil {
						continue
					}
					nodes = append(nodes, &Node{ID: nodeID, Props: props})
				}
				// Trust the index: if no entries match, the result is empty.
				return nil
			}

			// Slow path: no index — full scan fallback.
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				if val, ok := props[propName]; ok && fmt.Sprintf("%v", val) == fmt.Sprintf("%v", value) {
					nodeID := decodeNodeID(k)
					nodes = append(nodes, &Node{ID: nodeID, Props: props})
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// DropIndex removes a secondary index on a property.
func (db *DB) DropIndex(propName string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}

	prefix := []byte(propName + ":")

	for _, s := range db.shards {
		err := s.db.Update(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			c := idxBucket.Cursor()

			var toDelete [][]byte
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				toDelete = append(toDelete, keyCopy)
			}

			for _, k := range toDelete {
				if err := idxBucket.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to drop index on %s: %w", propName, err)
		}
	}

	// Remove from in-memory tracking.
	db.indexedProps.Delete(propName)
	return nil
}

// ReIndex rebuilds the property index from scratch.
// Useful after bulk inserts where index maintenance was skipped.
func (db *DB) ReIndex(propName string) error {
	if err := db.DropIndex(propName); err != nil {
		return err
	}
	return db.CreateIndex(propName)
}

// ---------------------------------------------------------------------------
// Index maintenance helpers — called inside bbolt write transactions by
// AddNode, UpdateNode, SetNodeProps, DeleteNode to keep indexes up-to-date.
// ---------------------------------------------------------------------------

// indexNodeProps adds index entries for all indexed properties of a node.
// Must be called within a bbolt Update transaction on the correct shard.
func (db *DB) indexNodeProps(tx *bolt.Tx, nodeID NodeID, props Props) error {
	if len(props) == 0 {
		return nil
	}
	idxBucket := tx.Bucket(bucketIdxProp)

	var firstErr error
	db.indexedProps.Range(func(key, _ any) bool {
		propName := key.(string)
		val, ok := props[propName]
		if !ok {
			return true // property not present in this node
		}
		idxKeyStr := fmt.Sprintf("%s:%v", propName, val)
		idxKey := encodeIndexKey(idxKeyStr, uint64(nodeID))
		if err := idxBucket.Put(idxKey, nil); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}

// unindexNodeProps removes index entries for all indexed properties of a node.
// Must be called within a bbolt Update transaction on the correct shard.
func (db *DB) unindexNodeProps(tx *bolt.Tx, nodeID NodeID, props Props) error {
	if len(props) == 0 {
		return nil
	}
	idxBucket := tx.Bucket(bucketIdxProp)

	var firstErr error
	db.indexedProps.Range(func(key, _ any) bool {
		propName := key.(string)
		val, ok := props[propName]
		if !ok {
			return true
		}
		idxKeyStr := fmt.Sprintf("%s:%v", propName, val)
		idxKey := encodeIndexKey(idxKeyStr, uint64(nodeID))
		if err := idxBucket.Delete(idxKey); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}
