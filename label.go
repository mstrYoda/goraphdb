package graphdb

import (
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

// AddNodeWithLabels creates a new node with labels and properties.
// Labels are stored separately from properties for efficient label-based lookups.
func (db *DB) AddNodeWithLabels(labels []string, props Props) (NodeID, error) {
	if db.isClosed() {
		return 0, fmt.Errorf("graphdb: database is closed")
	}

	s := db.primaryShard()
	id := s.allocNodeID()
	target := db.shardFor(id)

	data, err := encodeProps(props)
	if err != nil {
		return 0, fmt.Errorf("graphdb: failed to encode node properties: %w", err)
	}

	var labelData []byte
	if len(labels) > 0 {
		labelData, err = msgpack.Marshal(labels)
		if err != nil {
			return 0, fmt.Errorf("graphdb: failed to encode labels: %w", err)
		}
	}

	err = target.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketNodes).Put(encodeNodeID(id), data); err != nil {
			return err
		}
		if err := db.indexNodeProps(tx, id, props); err != nil {
			return err
		}
		// Store labels.
		if len(labelData) > 0 {
			if err := tx.Bucket(bucketNodeLabels).Put(encodeNodeID(id), labelData); err != nil {
				return err
			}
			// Maintain label index.
			idxBucket := tx.Bucket(bucketIdxNodeLabel)
			for _, label := range labels {
				if err := idxBucket.Put(encodeLabelIndexKey(label, id), nil); err != nil {
					return err
				}
			}
		}
		target.nodeCount.Add(1)
		return nil
	})
	if err != nil {
		db.log.Error("failed to add node with labels", "error", err)
		return 0, fmt.Errorf("graphdb: failed to add node: %w", err)
	}

	db.walAppend(OpAddNodeWithLabels, WALAddNodeWithLabels{ID: id, Labels: labels, Props: props})
	db.ncache.Put(&Node{ID: id, Labels: labels, Props: props})
	db.log.Debug("node added", "id", id, "labels", labels)
	return id, nil
}

// AddLabel adds one or more labels to an existing node.
func (db *DB) AddLabel(id NodeID, labels ...string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if len(labels) == 0 {
		return nil
	}

	s := db.shardFor(id)
	err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		// Verify node exists.
		if tx.Bucket(bucketNodes).Get(encodeNodeID(id)) == nil {
			return fmt.Errorf("graphdb: node %d not found", id)
		}

		// Load existing labels.
		existing := loadLabels(tx, id)

		// Merge: add only new labels.
		labelSet := make(map[string]bool, len(existing))
		for _, l := range existing {
			labelSet[l] = true
		}
		var added []string
		for _, l := range labels {
			if !labelSet[l] {
				existing = append(existing, l)
				added = append(added, l)
			}
		}
		if len(added) == 0 {
			return nil // nothing new
		}

		// Persist labels.
		data, err := msgpack.Marshal(existing)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketNodeLabels).Put(encodeNodeID(id), data); err != nil {
			return err
		}

		// Add index entries for new labels.
		idxBucket := tx.Bucket(bucketIdxNodeLabel)
		for _, l := range added {
			if err := idxBucket.Put(encodeLabelIndexKey(l, id), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.log.Error("failed to add labels", "id", id, "labels", labels, "error", err)
	} else {
		db.walAppend(OpAddLabel, WALAddLabel{ID: id, Labels: labels})
		db.ncache.Invalidate(id)
		db.log.Debug("labels added", "id", id, "labels", labels)
	}
	return err
}

// RemoveLabel removes one or more labels from an existing node.
func (db *DB) RemoveLabel(id NodeID, labels ...string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if len(labels) == 0 {
		return nil
	}

	s := db.shardFor(id)
	err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		if tx.Bucket(bucketNodes).Get(encodeNodeID(id)) == nil {
			return fmt.Errorf("graphdb: node %d not found", id)
		}

		existing := loadLabels(tx, id)
		removeSet := make(map[string]bool, len(labels))
		for _, l := range labels {
			removeSet[l] = true
		}

		var kept []string
		for _, l := range existing {
			if !removeSet[l] {
				kept = append(kept, l)
			}
		}

		// Persist updated labels.
		if len(kept) == 0 {
			if err := tx.Bucket(bucketNodeLabels).Delete(encodeNodeID(id)); err != nil {
				return err
			}
		} else {
			data, err := msgpack.Marshal(kept)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bucketNodeLabels).Put(encodeNodeID(id), data); err != nil {
				return err
			}
		}

		// Remove index entries.
		idxBucket := tx.Bucket(bucketIdxNodeLabel)
		for _, l := range labels {
			if err := idxBucket.Delete(encodeLabelIndexKey(l, id)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.log.Error("failed to remove labels", "id", id, "labels", labels, "error", err)
	} else {
		db.walAppend(OpRemoveLabel, WALRemoveLabel{ID: id, Labels: labels})
		db.ncache.Invalidate(id)
		db.log.Debug("labels removed", "id", id, "labels", labels)
	}
	return err
}

// GetLabels returns the labels for a node.
func (db *DB) GetLabels(id NodeID) ([]string, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	s := db.shardFor(id)
	var labels []string
	err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketNodes).Get(encodeNodeID(id)) == nil {
			return fmt.Errorf("graphdb: node %d not found", id)
		}
		labels = loadLabels(tx, id)
		return nil
	})
	return labels, err
}

// FindByLabel returns all nodes that have the given label.
// Uses the idx_node_label index for O(matches) performance.
func (db *DB) FindByLabel(label string) ([]*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	prefix := encodeLabelIndexPrefix(label)
	var nodes []*Node

	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxNodeLabel)
			nodeBucket := tx.Bucket(bucketNodes)
			labelBucket := tx.Bucket(bucketNodeLabels)

			c := idxBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
				nodeIDBytes := k[len(prefix):]
				if len(nodeIDBytes) < 8 {
					continue
				}
				nodeID := decodeNodeID(nodeIDBytes)

				data := nodeBucket.Get(encodeNodeID(nodeID))
				if data == nil {
					continue
				}
				props, err := decodeProps(data)
				if err != nil {
					continue
				}

				var labels []string
				if ldata := labelBucket.Get(encodeNodeID(nodeID)); ldata != nil {
					_ = msgpack.Unmarshal(ldata, &labels)
				}

				nodes = append(nodes, &Node{ID: nodeID, Labels: labels, Props: props})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// HasLabel checks if a node has a specific label.
func (db *DB) HasLabel(id NodeID, label string) (bool, error) {
	labels, err := db.GetLabels(id)
	if err != nil {
		return false, err
	}
	for _, l := range labels {
		if l == label {
			return true, nil
		}
	}
	return false, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// encodeLabelIndexKey creates: "Label\x00" + nodeID(8 bytes big-endian)
func encodeLabelIndexKey(label string, id NodeID) []byte {
	b := []byte(label)
	key := make([]byte, len(b)+1+8)
	copy(key, b)
	key[len(b)] = 0x00
	encodeUint64Into(key[len(b)+1:], uint64(id))
	return key
}

// encodeLabelIndexPrefix creates: "Label\x00"
func encodeLabelIndexPrefix(label string) []byte {
	b := []byte(label)
	prefix := make([]byte, len(b)+1)
	copy(prefix, b)
	prefix[len(b)] = 0x00
	return prefix
}

// encodeUint64Into writes a uint64 big-endian into an existing slice.
func encodeUint64Into(buf []byte, v uint64) {
	buf[0] = byte(v >> 56)
	buf[1] = byte(v >> 48)
	buf[2] = byte(v >> 40)
	buf[3] = byte(v >> 32)
	buf[4] = byte(v >> 24)
	buf[5] = byte(v >> 16)
	buf[6] = byte(v >> 8)
	buf[7] = byte(v)
}

// hasPrefix is a local helper to avoid importing bytes in this file.
func hasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i, b := range prefix {
		if s[i] != b {
			return false
		}
	}
	return true
}

// loadLabels reads labels for a node from the node_labels bucket.
func loadLabels(tx *bolt.Tx, id NodeID) []string {
	data := tx.Bucket(bucketNodeLabels).Get(encodeNodeID(id))
	if data == nil {
		return nil
	}
	var labels []string
	if err := msgpack.Unmarshal(data, &labels); err != nil {
		return nil
	}
	return labels
}
