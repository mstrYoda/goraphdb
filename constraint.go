package graphdb

// ---------------------------------------------------------------------------
// Unique Constraints — enforce value uniqueness for (label, property) pairs.
//
// A unique constraint guarantees that no two nodes with the same label can
// share the same value for a given property. For example:
//
//	db.CreateUniqueConstraint("Person", "email")
//
// After this, any attempt to create two Person nodes with the same email
// will fail with ErrUniqueConstraintViolation.
//
// Architecture:
//
//	┌──────────────────────────────────────────────────────────────┐
//	│                    Unique Constraint System                   │
//	│                                                              │
//	│  In-memory registry (sync.Map)                               │
//	│    uniqueConstraints: "Person\x00email" → UniqueConstraint   │
//	│                                                              │
//	│  bbolt storage (per-shard):                                  │
//	│    ┌─────────────────────────────────────────────────────┐   │
//	│    │ bucket: unique_meta                                  │   │
//	│    │   key: "Person\x00email" → nil  (constraint exists)  │   │
//	│    └─────────────────────────────────────────────────────┘   │
//	│    ┌─────────────────────────────────────────────────────┐   │
//	│    │ bucket: idx_unique                                   │   │
//	│    │   key: "Person\x00email\x00alice@ex.com" → nodeID   │   │
//	│    │   key: "Person\x00email\x00bob@ex.com"   → nodeID   │   │
//	│    └─────────────────────────────────────────────────────┘   │
//	│                                                              │
//	│  Enforcement points:                                         │
//	│    • AddNodeWithLabels — check before insert                 │
//	│    • AddLabel          — check when label added to node      │
//	│    • UpdateNode        — check if constrained prop changes   │
//	│    • SetNodeProps      — check if constrained prop changes   │
//	│    • Cypher CREATE     — via AddNodeWithLabels               │
//	│    • Cypher MERGE      — lookup by constraint, then create   │
//	│                                                              │
//	│  Concurrency:                                                │
//	│    Single-shard: fully serialized by bbolt's writer lock     │
//	│    Multi-shard:  check spans shards (eventual consistency)   │
//	└──────────────────────────────────────────────────────────────┘
//
// The unique index is stored per-shard alongside node data. On a single-shard
// database (the default), uniqueness is guaranteed by bbolt's single-writer
// serialization. On multi-shard databases, there is a small race window
// between the cross-shard check and the insert — this is a documented
// limitation accepted for the performance benefit of shard-local indexes.
// ---------------------------------------------------------------------------

import (
	"bytes"
	"context"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// ErrUniqueConstraintViolation is returned when a write would create a
// duplicate value for a uniquely-constrained (label, property) pair.
var ErrUniqueConstraintViolation = fmt.Errorf("graphdb: unique constraint violation")

// UniqueConstraint defines a uniqueness rule: no two nodes with the given
// label may share the same value for the given property.
type UniqueConstraint struct {
	Label    string `json:"label"`
	Property string `json:"property"`
}

// constraintKey returns a canonical string key for in-memory maps.
func (uc UniqueConstraint) constraintKey() string {
	return uc.Label + "\x00" + uc.Property
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// CreateUniqueConstraint creates a unique constraint ensuring that no two
// nodes with the given label share the same value for the given property.
//
// The method scans all existing nodes with the label and builds the unique
// index. If any duplicates are found among existing data, the constraint
// creation fails with ErrUniqueConstraintViolation.
//
// Example:
//
//	err := db.CreateUniqueConstraint("Person", "email")
//	// Now: CREATE (n:Person {email: "alice@ex.com"}) twice → error
func (db *DB) CreateUniqueConstraint(label, property string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	uc := UniqueConstraint{Label: label, Property: property}

	// Check if constraint already exists.
	if _, loaded := db.uniqueConstraints.Load(uc.constraintKey()); loaded {
		return nil // idempotent
	}

	// Phase 1: scan existing nodes to build unique index and check for duplicates.
	// Collect all (valueStr → nodeID) pairs for nodes with this label+property.
	seen := make(map[string]NodeID)

	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			lblBucket := tx.Bucket(bucketIdxNodeLabel)
			nodesBucket := tx.Bucket(bucketNodes)

			prefix := encodeLabelIndexPrefix(label)
			c := lblBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
				// Extract nodeID from label index key: "Label\x00" + nodeID(8)
				if len(k) < len(prefix)+8 {
					continue
				}
				nodeID := decodeNodeID(k[len(prefix):])

				data := nodesBucket.Get(encodeNodeID(nodeID))
				if data == nil {
					continue
				}
				props, err := decodeProps(data)
				if err != nil {
					continue
				}

				val, ok := props[property]
				if !ok {
					continue
				}

				valStr := fmt.Sprintf("%v", val)
				if existingID, dup := seen[valStr]; dup {
					return fmt.Errorf("%w: label=%s property=%s value=%q (nodes %d and %d)",
						ErrUniqueConstraintViolation, label, property, valStr, existingID, nodeID)
				}
				seen[valStr] = nodeID
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Phase 2: write unique index entries into each shard (co-located with node data).
	for valStr, nodeID := range seen {
		target := db.shardFor(nodeID)
		err := target.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxKey := encodeUniqueKey(label, property, valStr)
			return tx.Bucket(bucketIdxUnique).Put(idxKey, encodeNodeID(nodeID))
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to build unique index: %w", err)
		}
	}

	// Phase 3: persist constraint metadata in primary shard.
	err := db.primaryShard().writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		metaKey := encodeConstraintMetaKey(label, property)
		return tx.Bucket(bucketUniqueMeta).Put(metaKey, nil)
	})
	if err != nil {
		return fmt.Errorf("graphdb: failed to persist constraint metadata: %w", err)
	}

	// Register in memory.
	db.uniqueConstraints.Store(uc.constraintKey(), uc)

	// WAL: record for replication.
	db.walAppend(OpCreateUniqueConstraint, WALCreateUniqueConstraint{
		Label:    label,
		Property: property,
	})

	db.log.Info("unique constraint created", "label", label, "property", property)
	return nil
}

// DropUniqueConstraint removes a unique constraint on (label, property).
// The constraint metadata and all unique index entries are removed.
func (db *DB) DropUniqueConstraint(label, property string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	uc := UniqueConstraint{Label: label, Property: property}

	if _, loaded := db.uniqueConstraints.Load(uc.constraintKey()); !loaded {
		return nil // doesn't exist — idempotent
	}

	// Remove all unique index entries across all shards.
	prefix := encodeUniquePrefix(label, property)
	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxUnique)
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
			return err
		}
	}

	// Remove metadata from primary shard.
	err := db.primaryShard().writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		return tx.Bucket(bucketUniqueMeta).Delete(encodeConstraintMetaKey(label, property))
	})
	if err != nil {
		return err
	}

	// Unregister from memory.
	db.uniqueConstraints.Delete(uc.constraintKey())

	// WAL
	db.walAppend(OpDropUniqueConstraint, WALDropUniqueConstraint{
		Label:    label,
		Property: property,
	})

	db.log.Info("unique constraint dropped", "label", label, "property", property)
	return nil
}

// HasUniqueConstraint returns true if a unique constraint exists on (label, property).
func (db *DB) HasUniqueConstraint(label, property string) bool {
	uc := UniqueConstraint{Label: label, Property: property}
	_, ok := db.uniqueConstraints.Load(uc.constraintKey())
	return ok
}

// ListUniqueConstraints returns all registered unique constraints.
func (db *DB) ListUniqueConstraints() []UniqueConstraint {
	var result []UniqueConstraint
	db.uniqueConstraints.Range(func(_, value any) bool {
		result = append(result, value.(UniqueConstraint))
		return true
	})
	return result
}

// ---------------------------------------------------------------------------
// Internal enforcement helpers
// ---------------------------------------------------------------------------

// checkUniqueConstraints verifies that creating a node with the given labels
// and properties would not violate any unique constraints. If excludeID is
// non-zero, that node ID is excluded from the check (used for updates).
//
// Returns ErrUniqueConstraintViolation if a duplicate is found.
func (db *DB) checkUniqueConstraints(labels []string, props Props, excludeID NodeID) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}

	for _, label := range labels {
		var checkErr error
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true // different label
			}

			val, ok := props[uc.Property]
			if !ok {
				return true // property not present
			}

			valStr := fmt.Sprintf("%v", val)
			existingID, found := db.lookupUniqueIndex(label, uc.Property, valStr)
			if found && existingID != excludeID {
				checkErr = fmt.Errorf("%w: label=%s property=%s value=%q already exists (node %d)",
					ErrUniqueConstraintViolation, label, uc.Property, valStr, existingID)
				return false // stop iteration
			}
			return true
		})
		if checkErr != nil {
			return checkErr
		}
	}
	return nil
}

// checkUniqueConstraintsInTx is like checkUniqueConstraints but also checks
// within the given bolt transaction (for the current shard). This provides
// stronger consistency for single-shard databases where the check and insert
// happen in the same transaction.
func (db *DB) checkUniqueConstraintsInTx(tx *bolt.Tx, labels []string, props Props, excludeID NodeID) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}

	idxBucket := tx.Bucket(bucketIdxUnique)
	if idxBucket == nil {
		return nil
	}

	for _, label := range labels {
		var checkErr error
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true
			}

			val, ok := props[uc.Property]
			if !ok {
				return true
			}

			valStr := fmt.Sprintf("%v", val)
			idxKey := encodeUniqueKey(label, uc.Property, valStr)

			existing := idxBucket.Get(idxKey)
			if existing != nil {
				existingID := decodeNodeID(existing)
				if existingID != excludeID {
					checkErr = fmt.Errorf("%w: label=%s property=%s value=%q already exists (node %d)",
						ErrUniqueConstraintViolation, label, uc.Property, valStr, existingID)
					return false
				}
			}
			return true
		})
		if checkErr != nil {
			return checkErr
		}
	}
	return nil
}

// indexUniqueConstraints updates the unique index for a node within a bolt tx.
// Called after successfully inserting/updating a node.
func (db *DB) indexUniqueConstraints(tx *bolt.Tx, nodeID NodeID, labels []string, props Props) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}

	idxBucket := tx.Bucket(bucketIdxUnique)
	if idxBucket == nil {
		return nil
	}

	for _, label := range labels {
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true
			}
			val, ok := props[uc.Property]
			if !ok {
				return true
			}
			valStr := fmt.Sprintf("%v", val)
			idxKey := encodeUniqueKey(label, uc.Property, valStr)
			_ = idxBucket.Put(idxKey, encodeNodeID(nodeID))
			return true
		})
	}
	return nil
}

// unindexUniqueConstraints removes unique index entries for a node within a tx.
// Called before deleting/updating a node.
func (db *DB) unindexUniqueConstraints(tx *bolt.Tx, nodeID NodeID, labels []string, props Props) error {
	if len(labels) == 0 || len(props) == 0 {
		return nil
	}

	idxBucket := tx.Bucket(bucketIdxUnique)
	if idxBucket == nil {
		return nil
	}

	for _, label := range labels {
		db.uniqueConstraints.Range(func(_, value any) bool {
			uc := value.(UniqueConstraint)
			if uc.Label != label {
				return true
			}
			val, ok := props[uc.Property]
			if !ok {
				return true
			}
			valStr := fmt.Sprintf("%v", val)
			idxKey := encodeUniqueKey(label, uc.Property, valStr)

			// Only delete if this node owns the index entry.
			existing := idxBucket.Get(idxKey)
			if existing != nil && decodeNodeID(existing) == nodeID {
				_ = idxBucket.Delete(idxKey)
			}
			return true
		})
	}
	return nil
}

// lookupUniqueIndex searches all shards for a unique index entry.
// Returns (nodeID, true) if found, or (0, false) if not.
func (db *DB) lookupUniqueIndex(label, property, valueStr string) (NodeID, bool) {
	idxKey := encodeUniqueKey(label, property, valueStr)

	for _, s := range db.shards {
		var found bool
		var id NodeID
		_ = s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketIdxUnique)
			if b == nil {
				return nil
			}
			v := b.Get(idxKey)
			if v != nil {
				id = decodeNodeID(v)
				found = true
			}
			return nil
		})
		if found {
			return id, true
		}
	}
	return 0, false
}

// FindByUniqueConstraint looks up a single node by a unique constraint.
// This is O(1) per shard — significantly faster than FindByProperty for
// the common "find by primary key" pattern.
//
// Returns the node if found, or nil if no node matches.
func (db *DB) FindByUniqueConstraint(label, property string, value any) (*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	valStr := fmt.Sprintf("%v", value)
	nodeID, found := db.lookupUniqueIndex(label, property, valStr)
	if !found {
		return nil, nil
	}

	return db.getNode(nodeID)
}

// ---------------------------------------------------------------------------
// Constraint discovery on startup
// ---------------------------------------------------------------------------

// discoverUniqueConstraints loads unique constraint metadata from disk.
// Called during Open() to restore the in-memory constraint registry.
func (db *DB) discoverUniqueConstraints() {
	s := db.primaryShard()
	_ = s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketUniqueMeta)
		if meta == nil {
			return nil
		}
		return meta.ForEach(func(k, _ []byte) error {
			label, property := decodeConstraintMetaKey(k)
			if label != "" && property != "" {
				uc := UniqueConstraint{Label: label, Property: property}
				db.uniqueConstraints.Store(uc.constraintKey(), uc)
			}
			return nil
		})
	})
}

// ---------------------------------------------------------------------------
// Key encoding helpers
// ---------------------------------------------------------------------------

// encodeUniqueKey creates: "Label\x00Property\x00ValueStr"
func encodeUniqueKey(label, property, valueStr string) []byte {
	// label + \x00 + property + \x00 + valueStr
	key := make([]byte, len(label)+1+len(property)+1+len(valueStr))
	n := copy(key, label)
	key[n] = 0x00
	n++
	n += copy(key[n:], property)
	key[n] = 0x00
	n++
	copy(key[n:], valueStr)
	return key
}

// encodeUniquePrefix creates: "Label\x00Property\x00" for prefix scanning.
func encodeUniquePrefix(label, property string) []byte {
	prefix := make([]byte, len(label)+1+len(property)+1)
	n := copy(prefix, label)
	prefix[n] = 0x00
	n++
	n += copy(prefix[n:], property)
	prefix[n] = 0x00
	return prefix
}

// encodeConstraintMetaKey creates: "Label\x00Property"
func encodeConstraintMetaKey(label, property string) []byte {
	key := make([]byte, len(label)+1+len(property))
	n := copy(key, label)
	key[n] = 0x00
	n++
	copy(key[n:], property)
	return key
}

// decodeConstraintMetaKey splits "Label\x00Property" back into components.
func decodeConstraintMetaKey(key []byte) (label, property string) {
	idx := bytes.IndexByte(key, 0x00)
	if idx < 0 {
		return "", ""
	}
	return string(key[:idx]), string(key[idx+1:])
}
