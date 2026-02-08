package graphdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// compositeIndexDef represents a composite index definition.
// Properties are stored in sorted order for canonical key generation.
type compositeIndexDef struct {
	Properties []string // sorted property names
	Key        string   // canonical key: "prop1\x00prop2" (used as map key)
}

// newCompositeIndexDef creates a canonical composite index definition.
func newCompositeIndexDef(props []string) compositeIndexDef {
	sorted := make([]string, len(props))
	copy(sorted, props)
	sort.Strings(sorted)
	return compositeIndexDef{
		Properties: sorted,
		Key:        strings.Join(sorted, "\x00"),
	}
}

// CreateCompositeIndex creates a secondary index on multiple properties.
// This allows fast lookups when querying by all properties in the composite key.
//
// Example:
//
//	db.CreateCompositeIndex("city", "age")
//	nodes, _ := db.FindByCompositeIndex(map[string]any{"city": "Istanbul", "age": 30})
func (db *DB) CreateCompositeIndex(propNames ...string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}
	if len(propNames) < 2 {
		return fmt.Errorf("graphdb: composite index requires at least 2 properties")
	}

	def := newCompositeIndexDef(propNames)

	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
			nodesBucket := tx.Bucket(bucketNodes)

			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil // skip corrupted entries
				}

				idxKey := buildCompositeKey(def, props, decodeNodeID(k))
				if idxKey == nil {
					return nil // node doesn't have all required properties
				}
				return idxBucket.Put(idxKey, nil)
			})
		})
		if err != nil {
			return fmt.Errorf("graphdb: failed to create composite index on %v: %w", propNames, err)
		}
	}

	db.compositeIndexes.Store(def.Key, def)
	db.walAppend(OpCreateCompositeIndex, WALCreateCompositeIndex{PropNames: propNames})
	db.log.Info("composite index created", "properties", def.Properties)
	return nil
}

// FindByCompositeIndex finds nodes where all specified properties match the given values.
// Uses the composite index if available, otherwise falls back to full scan.
func (db *DB) FindByCompositeIndex(filters map[string]any) ([]*Node, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if len(filters) < 2 {
		return nil, fmt.Errorf("graphdb: composite lookup requires at least 2 properties")
	}

	// Build a canonical key to check if a composite index exists.
	propNames := make([]string, 0, len(filters))
	for k := range filters {
		propNames = append(propNames, k)
	}
	def := newCompositeIndexDef(propNames)

	_, hasIndex := db.compositeIndexes.Load(def.Key)

	if hasIndex {
		return db.findByCompositeIndexFast(def, filters)
	}

	// Fallback: full scan.
	return db.findByCompositeIndexScan(filters)
}

// findByCompositeIndexFast uses the composite index bucket for a prefix seek.
func (db *DB) findByCompositeIndexFast(def compositeIndexDef, filters map[string]any) ([]*Node, error) {
	prefix := buildCompositePrefix(def, filters)
	if prefix == nil {
		return nil, fmt.Errorf("graphdb: could not build composite prefix for %v", def.Properties)
	}

	var nodes []*Node
	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
			nodesBucket := tx.Bucket(bucketNodes)

			c := idxBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				// Last 8 bytes are the nodeID.
				if len(k) < 8 {
					continue
				}
				nodeID := NodeID(binary.BigEndian.Uint64(k[len(k)-8:]))

				data := nodesBucket.Get(encodeNodeID(nodeID))
				if data == nil {
					continue
				}
				props, err := decodeProps(data)
				if err != nil {
					continue
				}
				labels := loadLabels(tx, nodeID)
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

// findByCompositeIndexScan is the full-scan fallback when no composite index exists.
func (db *DB) findByCompositeIndexScan(filters map[string]any) ([]*Node, error) {
	var nodes []*Node
	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			nodesBucket := tx.Bucket(bucketNodes)
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				for fk, fv := range filters {
					pv, ok := props[fk]
					if !ok || fmt.Sprintf("%v", pv) != fmt.Sprintf("%v", fv) {
						return nil
					}
				}
				nodeID := decodeNodeID(k)
				labels := loadLabels(tx, nodeID)
				nodes = append(nodes, &Node{ID: nodeID, Labels: labels, Props: props})
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

// DropCompositeIndex removes a composite index.
func (db *DB) DropCompositeIndex(propNames ...string) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}
	if err := db.writeGuard(); err != nil {
		return err
	}

	def := newCompositeIndexDef(propNames)
	prefix := compositeDefPrefix(def)

	for _, s := range db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
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
			return fmt.Errorf("graphdb: failed to drop composite index on %v: %w", propNames, err)
		}
	}

	db.compositeIndexes.Delete(def.Key)
	db.walAppend(OpDropCompositeIndex, WALDropCompositeIndex{PropNames: propNames})
	db.log.Info("composite index dropped", "properties", def.Properties)
	return nil
}

// ListCompositeIndexes returns all composite index definitions.
func (db *DB) ListCompositeIndexes() [][]string {
	var result [][]string
	db.compositeIndexes.Range(func(_, value any) bool {
		def := value.(compositeIndexDef)
		cp := make([]string, len(def.Properties))
		copy(cp, def.Properties)
		result = append(result, cp)
		return true
	})
	return result
}

// HasCompositeIndex checks whether a composite index exists for the given properties.
func (db *DB) HasCompositeIndex(propNames ...string) bool {
	def := newCompositeIndexDef(propNames)
	_, ok := db.compositeIndexes.Load(def.Key)
	return ok
}

// ---------------------------------------------------------------------------
// Composite index key encoding
//
// Key format:
//   defPrefix + val1_bytes + 0x00 + val2_bytes + 0x00 + ... + nodeID(8)
//
// defPrefix = "prop1\x00prop2\x00" (sorted property names, null-separated, trailing null)
// val_bytes = fmt.Sprintf("%v", val) for each property in sorted order
// ---------------------------------------------------------------------------

// compositeDefPrefix returns the definition prefix for a composite index.
// Format: "prop1\x00prop2\x00"
func compositeDefPrefix(def compositeIndexDef) []byte {
	var buf bytes.Buffer
	for _, p := range def.Properties {
		buf.WriteString(p)
		buf.WriteByte(0x00)
	}
	return buf.Bytes()
}

// buildCompositeKey builds a full composite index key for a node.
// Returns nil if the node doesn't have all required properties.
func buildCompositeKey(def compositeIndexDef, props Props, nodeID NodeID) []byte {
	var buf bytes.Buffer

	// Definition prefix.
	for _, p := range def.Properties {
		buf.WriteString(p)
		buf.WriteByte(0x00)
	}

	// Values in sorted property order.
	for _, p := range def.Properties {
		val, ok := props[p]
		if !ok {
			return nil // node doesn't have all properties
		}
		buf.WriteString(fmt.Sprintf("%v", val))
		buf.WriteByte(0x00)
	}

	// Append nodeID (8 bytes big-endian).
	var idBuf [8]byte
	binary.BigEndian.PutUint64(idBuf[:], uint64(nodeID))
	buf.Write(idBuf[:])

	return buf.Bytes()
}

// buildCompositePrefix builds the prefix for a composite index seek (without nodeID).
func buildCompositePrefix(def compositeIndexDef, filters map[string]any) []byte {
	var buf bytes.Buffer

	// Definition prefix.
	for _, p := range def.Properties {
		buf.WriteString(p)
		buf.WriteByte(0x00)
	}

	// Values in sorted property order.
	for _, p := range def.Properties {
		val, ok := filters[p]
		if !ok {
			return nil
		}
		buf.WriteString(fmt.Sprintf("%v", val))
		buf.WriteByte(0x00)
	}

	return buf.Bytes()
}

// ---------------------------------------------------------------------------
// Composite index maintenance — called inside bbolt write transactions.
// ---------------------------------------------------------------------------

// indexNodeComposite adds composite index entries for a node.
func (db *DB) indexNodeComposite(tx *bolt.Tx, nodeID NodeID, props Props) error {
	idxBucket := tx.Bucket(bucketIdxComposite)
	var firstErr error

	db.compositeIndexes.Range(func(_, value any) bool {
		def := value.(compositeIndexDef)
		key := buildCompositeKey(def, props, nodeID)
		if key == nil {
			return true // node doesn't have all required properties
		}
		if err := idxBucket.Put(key, nil); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}

// unindexNodeComposite removes composite index entries for a node.
func (db *DB) unindexNodeComposite(tx *bolt.Tx, nodeID NodeID, props Props) error {
	idxBucket := tx.Bucket(bucketIdxComposite)
	var firstErr error

	db.compositeIndexes.Range(func(_, value any) bool {
		def := value.(compositeIndexDef)
		key := buildCompositeKey(def, props, nodeID)
		if key == nil {
			return true
		}
		if err := idxBucket.Delete(key); err != nil {
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}

// discoverCompositeIndexes scans the idx_composite bucket to learn which
// composite indexes exist. Called at startup.
func (db *DB) discoverCompositeIndexes() {
	// We scan the bucket and extract unique definition prefixes.
	// A definition prefix ends right before the first value segment.
	// Since we know property names don't contain 0x00 bytes, we can detect
	// the pattern: propName1\x00propName2\x00 followed by value data.
	//
	// Strategy: read the first key per unique prefix by jumping.
	seen := make(map[string]bool)

	for _, s := range db.shards {
		_ = s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketIdxComposite)
			if b == nil {
				return nil
			}
			c := b.Cursor()
			for k, _ := c.First(); k != nil; {
				// Try to extract the def prefix from the key.
				defKey := extractCompositeDefKey(k)
				if defKey != "" && !seen[defKey] {
					seen[defKey] = true
					props := strings.Split(defKey, "\x00")
					// Remove empty trailing element from split.
					var clean []string
					for _, p := range props {
						if p != "" {
							clean = append(clean, p)
						}
					}
					if len(clean) >= 2 {
						def := newCompositeIndexDef(clean)
						db.compositeIndexes.Store(def.Key, def)
					}
				}
				// Jump to next prefix by seeking past current def prefix.
				prefix := compositeDefPrefix(newCompositeIndexDef(strings.Split(defKey, "\x00")))
				// Seek to prefix with last byte incremented to skip all entries.
				next := make([]byte, len(prefix))
				copy(next, prefix)
				// Increment last non-zero byte.
				for i := len(next) - 1; i >= 0; i-- {
					next[i]++
					if next[i] != 0 {
						break
					}
				}
				k, _ = c.Seek(next)
			}
			return nil
		})
	}
}

// extractCompositeDefKey extracts the definition portion from a composite index key.
// The definition is the sequence of "propName\x00" segments at the start.
// We detect the boundary by counting null bytes: property names come first, then values.
// Since we use sorted property names and each is followed by \x00, and values are also
// followed by \x00, we need metadata. We use a simple heuristic: property names are
// non-numeric ASCII identifiers while values can be anything.
//
// Better approach: store a metadata entry in the bucket. For now, we store a metadata
// key "__defs__" → msgpack array of composite index definitions.
func extractCompositeDefKey(k []byte) string {
	// Find the definition prefix by looking for the pattern of consecutive
	// null-separated segments that look like property names.
	// Property names: [a-zA-Z_][a-zA-Z0-9_]* followed by \x00
	// We keep consuming segments that look like property names.
	var props []string
	remaining := k
	for len(remaining) > 0 {
		idx := bytes.IndexByte(remaining, 0x00)
		if idx <= 0 {
			break
		}
		segment := string(remaining[:idx])
		if !looksLikePropName(segment) {
			break
		}
		props = append(props, segment)
		remaining = remaining[idx+1:]
	}
	if len(props) < 2 {
		return ""
	}
	return strings.Join(props, "\x00")
}

// looksLikePropName returns true if s looks like a valid property name identifier.
func looksLikePropName(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i, c := range s {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
		} else {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
				return false
			}
		}
	}
	return true
}
