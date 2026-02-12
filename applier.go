package graphdb

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

// ---------------------------------------------------------------------------
// WAL Applier — replays committed mutations on a follower replica.
//
// The applier takes WAL entries (received via log shipping from the leader)
// and applies them directly to the follower's bbolt shards, bypassing the
// public write API (which is blocked by writeGuard on followers).
//
// Architecture:
//
//	Leader                          Follower
//	┌─────────┐    gRPC stream     ┌──────────────────────┐
//	│   WAL   │ ──────────────────►│  Applier.Apply(entry) │
//	│ segment │                    │      │                │
//	│  files  │                    │      ▼                │
//	└─────────┘                    │  applyAddNode()       │
//	                               │  applyAddEdge()       │
//	                               │  applyDeleteNode()    │
//	                               │  ...                  │
//	                               │      │                │
//	                               │      ▼                │
//	                               │  bbolt shards         │
//	                               │  (same layout)        │
//	                               └──────────────────────┘
//
// Key properties:
//   - Deterministic replay: uses the exact IDs from the leader (no re-allocation)
//   - Idempotent: applying the same entry twice is safe (put is a no-op if same data)
//   - Sequential: entries must be applied in LSN order
//   - The applier tracks the last applied LSN so followers can resume after restart
//
// Thread safety:
//   - The Applier itself is NOT safe for concurrent Apply() calls.
//   - It is designed to be called from a single goroutine (the apply loop).
//   - Read queries can run concurrently on the follower via bbolt's MVCC.
// ---------------------------------------------------------------------------

// Applier replays WAL entries on a follower's local database.
type Applier struct {
	db         *DB
	appliedLSN atomic.Uint64 // last successfully applied LSN
}

// NewApplier creates an Applier for the given database.
// The database should be opened in follower mode (Role: "follower").
func NewApplier(db *DB) *Applier {
	return &Applier{db: db}
}

// AppliedLSN returns the last successfully applied LSN.
func (a *Applier) AppliedLSN() uint64 {
	return a.appliedLSN.Load()
}

// ResetLSN resets the applied LSN back to zero.
// This is called when the follower switches to a new leader whose WAL
// starts from LSN 1 (different LSN space than the previous leader).
func (a *Applier) ResetLSN() {
	a.appliedLSN.Store(0)
}

// Apply replays a single WAL entry on the follower's database.
// Entries must be applied in LSN order. Returns an error if the entry
// cannot be applied (e.g., corrupted payload). Skips entries that have
// already been applied (LSN <= appliedLSN).
func (a *Applier) Apply(entry *WALEntry) error {
	// Skip already-applied entries (idempotency).
	if entry.LSN <= a.appliedLSN.Load() {
		return nil
	}

	var err error
	switch entry.Op {
	case OpAddNode:
		err = a.applyAddNode(entry.Payload)
	case OpAddNodeBatch:
		err = a.applyAddNodeBatch(entry.Payload)
	case OpUpdateNode:
		err = a.applyUpdateNode(entry.Payload)
	case OpSetNodeProps:
		err = a.applySetNodeProps(entry.Payload)
	case OpDeleteNode:
		err = a.applyDeleteNode(entry.Payload)
	case OpAddEdge:
		err = a.applyAddEdge(entry.Payload)
	case OpAddEdgeBatch:
		err = a.applyAddEdgeBatch(entry.Payload)
	case OpDeleteEdge:
		err = a.applyDeleteEdge(entry.Payload)
	case OpUpdateEdge:
		err = a.applyUpdateEdge(entry.Payload)
	case OpAddNodeWithLabels:
		err = a.applyAddNodeWithLabels(entry.Payload)
	case OpAddLabel:
		err = a.applyAddLabel(entry.Payload)
	case OpRemoveLabel:
		err = a.applyRemoveLabel(entry.Payload)
	case OpCreateIndex:
		err = a.applyCreateIndex(entry.Payload)
	case OpDropIndex:
		err = a.applyDropIndex(entry.Payload)
	case OpCreateCompositeIndex:
		err = a.applyCreateCompositeIndex(entry.Payload)
	case OpDropCompositeIndex:
		err = a.applyDropCompositeIndex(entry.Payload)
	case OpCreateUniqueConstraint:
		err = a.applyCreateUniqueConstraint(entry.Payload)
	case OpDropUniqueConstraint:
		err = a.applyDropUniqueConstraint(entry.Payload)
	default:
		err = fmt.Errorf("applier: unknown op type %d", entry.Op)
	}

	if err != nil {
		return fmt.Errorf("applier: failed to apply LSN %d (%s): %w", entry.LSN, entry.Op, err)
	}

	a.appliedLSN.Store(entry.LSN)
	return nil
}

// ---------------------------------------------------------------------------
// Per-operation apply methods
//
// These bypass the public API (which has writeGuard) and write directly to
// bbolt using the exact IDs from the leader. No ID allocation happens here.
// ---------------------------------------------------------------------------

func (a *Applier) applyAddNode(payload []byte) error {
	var p WALAddNode
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	target := a.db.shardFor(p.ID)
	data, err := encodeProps(p.Props)
	if err != nil {
		return err
	}

	err = target.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketNodes).Put(encodeNodeID(p.ID), data); err != nil {
			return err
		}
		if err := a.db.indexNodeProps(tx, p.ID, p.Props); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		target.nodeCount.Add(1)
	}
	return err
}

func (a *Applier) applyAddNodeBatch(payload []byte) error {
	var p WALAddNodeBatch
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	// Group by shard.
	type shardEntry struct {
		id   NodeID
		data []byte
	}
	groups := make(map[int][]shardEntry)
	for _, n := range p.Nodes {
		data, err := encodeProps(n.Props)
		if err != nil {
			return err
		}
		idx := int(uint64(n.ID) % uint64(len(a.db.shards)))
		groups[idx] = append(groups[idx], shardEntry{id: n.ID, data: data})
	}

	for shardIdx, entries := range groups {
		target := a.db.shards[shardIdx]
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
			return err
		}
		target.nodeCount.Add(uint64(len(entries)))
	}
	return nil
}

func (a *Applier) applyUpdateNode(payload []byte) error {
	var p WALUpdateNode
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	s := a.db.shardFor(p.ID)
	return s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		key := encodeNodeID(p.ID)
		existing := b.Get(key)
		if existing == nil {
			return fmt.Errorf("applier: node %d not found", p.ID)
		}

		oldProps, err := decodeProps(existing)
		if err != nil {
			return err
		}
		if err := a.db.unindexNodeProps(tx, p.ID, oldProps); err != nil {
			return err
		}

		for k, v := range p.Props {
			oldProps[k] = v
		}

		data, err := encodeProps(oldProps)
		if err != nil {
			return err
		}
		if err := b.Put(key, data); err != nil {
			return err
		}
		return a.db.indexNodeProps(tx, p.ID, oldProps)
	})
}

func (a *Applier) applySetNodeProps(payload []byte) error {
	var p WALSetNodeProps
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	s := a.db.shardFor(p.ID)
	return s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		key := encodeNodeID(p.ID)
		existing := b.Get(key)
		if existing != nil {
			oldProps, err := decodeProps(existing)
			if err == nil {
				_ = a.db.unindexNodeProps(tx, p.ID, oldProps)
			}
		}

		data, err := encodeProps(p.Props)
		if err != nil {
			return err
		}
		if err := b.Put(key, data); err != nil {
			return err
		}
		return a.db.indexNodeProps(tx, p.ID, p.Props)
	})
}

func (a *Applier) applyDeleteNode(payload []byte) error {
	var p WALDeleteNode
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	s := a.db.shardFor(p.ID)
	err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		key := encodeNodeID(p.ID)

		existing := b.Get(key)
		if existing == nil {
			return nil // already deleted — idempotent
		}

		if props, err := decodeProps(existing); err == nil {
			_ = a.db.unindexNodeProps(tx, p.ID, props)
		}

		labels := loadLabels(tx, p.ID)
		if len(labels) > 0 {
			idxBucket := tx.Bucket(bucketIdxNodeLabel)
			for _, l := range labels {
				_ = idxBucket.Delete(encodeLabelIndexKey(l, p.ID))
			}
			_ = tx.Bucket(bucketNodeLabels).Delete(encodeNodeID(p.ID))
		}

		if err := b.Delete(key); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		s.nodeCount.Add(^uint64(0))
	}
	return err
}

func (a *Applier) applyAddEdge(payload []byte) error {
	var p WALAddEdge
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	edge := &Edge{ID: p.ID, From: p.From, To: p.To, Label: p.Label, Props: p.Props}
	srcShard := a.db.shardForEdge(p.From)
	dstShard := a.db.shardFor(p.To)

	if srcShard == dstShard {
		err := srcShard.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			edgeData, err := encodeEdge(edge)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bucketEdges).Put(encodeEdgeID(p.ID), edgeData); err != nil {
				return err
			}
			if err := tx.Bucket(bucketAdjOut).Put(
				encodeAdjKey(p.From, p.ID), encodeAdjValue(p.To, p.Label),
			); err != nil {
				return err
			}
			if err := tx.Bucket(bucketAdjIn).Put(
				encodeAdjKey(p.To, p.ID), encodeAdjValue(p.From, p.Label),
			); err != nil {
				return err
			}
			if err := tx.Bucket(bucketIdxEdgeTyp).Put(
				encodeIndexKey(p.Label, uint64(p.ID)), nil,
			); err != nil {
				return err
			}
			return nil
		})
		if err == nil {
			srcShard.edgeCount.Add(1)
		}
		return err
	}

	// Cross-shard: two transactions.
	err := srcShard.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		edgeData, err := encodeEdge(edge)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketEdges).Put(encodeEdgeID(p.ID), edgeData); err != nil {
			return err
		}
		if err := tx.Bucket(bucketAdjOut).Put(
			encodeAdjKey(p.From, p.ID), encodeAdjValue(p.To, p.Label),
		); err != nil {
			return err
		}
		if err := tx.Bucket(bucketIdxEdgeTyp).Put(
			encodeIndexKey(p.Label, uint64(p.ID)), nil,
		); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	srcShard.edgeCount.Add(1)

	return dstShard.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAdjIn).Put(
			encodeAdjKey(p.To, p.ID), encodeAdjValue(p.From, p.Label),
		)
	})
}

func (a *Applier) applyAddEdgeBatch(payload []byte) error {
	var p WALAddEdgeBatch
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	// Apply each edge individually (simpler than batching by shard for the applier).
	for _, e := range p.Edges {
		entryPayload, err := encodeWALPayload(WALAddEdge{
			ID: e.ID, From: e.From, To: e.To, Label: e.Label, Props: e.Props,
		})
		if err != nil {
			return err
		}
		if err := a.applyAddEdge(entryPayload); err != nil {
			return err
		}
	}
	return nil
}

func (a *Applier) applyDeleteEdge(payload []byte) error {
	var p WALDeleteEdge
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	srcShard := a.db.shardForEdge(p.From)
	dstShard := a.db.shardFor(p.To)

	err := srcShard.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		_ = tx.Bucket(bucketEdges).Delete(encodeEdgeID(p.ID))
		_ = tx.Bucket(bucketAdjOut).Delete(encodeAdjKey(p.From, p.ID))
		_ = tx.Bucket(bucketIdxEdgeTyp).Delete(encodeIndexKey(p.Label, uint64(p.ID)))
		return nil
	})
	if err != nil {
		return err
	}
	srcShard.edgeCount.Add(^uint64(0))

	return dstShard.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAdjIn).Delete(encodeAdjKey(p.To, p.ID))
	})
}

func (a *Applier) applyUpdateEdge(payload []byte) error {
	var p WALUpdateEdge
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	s := a.db.shardForEdge(p.From)
	return s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketEdges).Get(encodeEdgeID(p.ID))
		if data == nil {
			return fmt.Errorf("applier: edge %d not found", p.ID)
		}

		edge, err := decodeEdge(data)
		if err != nil {
			return err
		}
		if edge.Props == nil {
			edge.Props = make(Props)
		}
		for k, v := range p.Props {
			edge.Props[k] = v
		}

		newData, err := encodeEdge(edge)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketEdges).Put(encodeEdgeID(p.ID), newData)
	})
}

func (a *Applier) applyAddNodeWithLabels(payload []byte) error {
	var p WALAddNodeWithLabels
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	target := a.db.shardFor(p.ID)
	data, err := encodeProps(p.Props)
	if err != nil {
		return err
	}

	var labelData []byte
	if len(p.Labels) > 0 {
		labelData, err = msgpack.Marshal(p.Labels)
		if err != nil {
			return err
		}
	}

	err = target.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketNodes).Put(encodeNodeID(p.ID), data); err != nil {
			return err
		}
		if err := a.db.indexNodeProps(tx, p.ID, p.Props); err != nil {
			return err
		}
		if len(labelData) > 0 {
			if err := tx.Bucket(bucketNodeLabels).Put(encodeNodeID(p.ID), labelData); err != nil {
				return err
			}
			idxBucket := tx.Bucket(bucketIdxNodeLabel)
			for _, label := range p.Labels {
				if err := idxBucket.Put(encodeLabelIndexKey(label, p.ID), nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err == nil {
		target.nodeCount.Add(1)
	}
	return err
}

func (a *Applier) applyAddLabel(payload []byte) error {
	var p WALAddLabel
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	s := a.db.shardFor(p.ID)
	return s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		if tx.Bucket(bucketNodes).Get(encodeNodeID(p.ID)) == nil {
			return fmt.Errorf("applier: node %d not found", p.ID)
		}

		existing := loadLabels(tx, p.ID)
		labelSet := make(map[string]bool, len(existing))
		for _, l := range existing {
			labelSet[l] = true
		}

		var added []string
		for _, l := range p.Labels {
			if !labelSet[l] {
				existing = append(existing, l)
				added = append(added, l)
			}
		}
		if len(added) == 0 {
			return nil
		}

		labelData, err := msgpack.Marshal(existing)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketNodeLabels).Put(encodeNodeID(p.ID), labelData); err != nil {
			return err
		}
		idxBucket := tx.Bucket(bucketIdxNodeLabel)
		for _, l := range added {
			if err := idxBucket.Put(encodeLabelIndexKey(l, p.ID), nil); err != nil {
				return err
			}
		}
		return nil
	})
}

func (a *Applier) applyRemoveLabel(payload []byte) error {
	var p WALRemoveLabel
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	s := a.db.shardFor(p.ID)
	return s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		if tx.Bucket(bucketNodes).Get(encodeNodeID(p.ID)) == nil {
			return nil // node already gone — idempotent
		}

		existing := loadLabels(tx, p.ID)
		removeSet := make(map[string]bool, len(p.Labels))
		for _, l := range p.Labels {
			removeSet[l] = true
		}

		var kept []string
		for _, l := range existing {
			if !removeSet[l] {
				kept = append(kept, l)
			}
		}

		if len(kept) == 0 {
			_ = tx.Bucket(bucketNodeLabels).Delete(encodeNodeID(p.ID))
		} else {
			labelData, err := msgpack.Marshal(kept)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bucketNodeLabels).Put(encodeNodeID(p.ID), labelData); err != nil {
				return err
			}
		}

		idxBucket := tx.Bucket(bucketIdxNodeLabel)
		for _, l := range p.Labels {
			_ = idxBucket.Delete(encodeLabelIndexKey(l, p.ID))
		}
		return nil
	})
}

func (a *Applier) applyCreateIndex(payload []byte) error {
	var p WALCreateIndex
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	// Build the index on the follower (same logic as leader).
	for _, s := range a.db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			nodesBucket := tx.Bucket(bucketNodes)
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				val, ok := props[p.PropName]
				if !ok {
					return nil
				}
				idxKeyStr := fmt.Sprintf("%s:%v", p.PropName, val)
				nodeID := decodeNodeID(k)
				idxKey := encodeIndexKey(idxKeyStr, uint64(nodeID))
				return idxBucket.Put(idxKey, nil)
			})
		})
		if err != nil {
			return err
		}
	}
	a.db.indexedProps.Store(p.PropName, true)
	return nil
}

func (a *Applier) applyDropIndex(payload []byte) error {
	var p WALDropIndex
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	prefix := []byte(p.PropName + ":")
	for _, s := range a.db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxProp)
			c := idxBucket.Cursor()
			var toDelete [][]byte
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
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
	a.db.indexedProps.Delete(p.PropName)
	return nil
}

func (a *Applier) applyCreateCompositeIndex(payload []byte) error {
	var p WALCreateCompositeIndex
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	def := newCompositeIndexDef(p.PropNames)
	for _, s := range a.db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
			nodesBucket := tx.Bucket(bucketNodes)
			return nodesBucket.ForEach(func(k, v []byte) error {
				props, err := decodeProps(v)
				if err != nil {
					return nil
				}
				idxKey := buildCompositeKey(def, props, decodeNodeID(k))
				if idxKey == nil {
					return nil
				}
				return idxBucket.Put(idxKey, nil)
			})
		})
		if err != nil {
			return err
		}
	}
	a.db.compositeIndexes.Store(def.Key, def)
	return nil
}

func (a *Applier) applyCreateUniqueConstraint(payload []byte) error {
	var p WALCreateUniqueConstraint
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}
	// Re-run CreateUniqueConstraint logic on the follower.
	// We bypass writeGuard because the applier is allowed to write.
	uc := UniqueConstraint{Label: p.Label, Property: p.Property}
	if _, loaded := a.db.uniqueConstraints.Load(uc.constraintKey()); loaded {
		return nil // already exists
	}

	// Build unique index from existing data.
	seen := make(map[string]NodeID)
	for _, s := range a.db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			lblBucket := tx.Bucket(bucketIdxNodeLabel)
			nodesBucket := tx.Bucket(bucketNodes)
			prefix := encodeLabelIndexPrefix(p.Label)
			c := lblBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
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
				val, ok := props[p.Property]
				if !ok {
					continue
				}
				valStr := fmt.Sprintf("%v", val)
				seen[valStr] = nodeID
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	for valStr, nodeID := range seen {
		target := a.db.shardFor(nodeID)
		err := target.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxKey := encodeUniqueKey(p.Label, p.Property, valStr)
			return tx.Bucket(bucketIdxUnique).Put(idxKey, encodeNodeID(nodeID))
		})
		if err != nil {
			return err
		}
	}

	err := a.db.primaryShard().writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		return tx.Bucket(bucketUniqueMeta).Put(encodeConstraintMetaKey(p.Label, p.Property), nil)
	})
	if err != nil {
		return err
	}

	a.db.uniqueConstraints.Store(uc.constraintKey(), uc)
	return nil
}

func (a *Applier) applyDropUniqueConstraint(payload []byte) error {
	var p WALDropUniqueConstraint
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}
	uc := UniqueConstraint{Label: p.Label, Property: p.Property}
	if _, loaded := a.db.uniqueConstraints.Load(uc.constraintKey()); !loaded {
		return nil
	}

	prefix := encodeUniquePrefix(p.Label, p.Property)
	for _, s := range a.db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxUnique)
			c := idxBucket.Cursor()
			var toDelete [][]byte
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
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

	_ = a.db.primaryShard().writeUpdate(context.Background(), func(tx *bolt.Tx) error {
		return tx.Bucket(bucketUniqueMeta).Delete(encodeConstraintMetaKey(p.Label, p.Property))
	})

	a.db.uniqueConstraints.Delete(uc.constraintKey())
	return nil
}

func (a *Applier) applyDropCompositeIndex(payload []byte) error {
	var p WALDropCompositeIndex
	if err := msgpack.Unmarshal(payload, &p); err != nil {
		return err
	}

	def := newCompositeIndexDef(p.PropNames)
	prefix := compositeDefPrefix(def)
	for _, s := range a.db.shards {
		err := s.writeUpdate(context.Background(), func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxComposite)
			c := idxBucket.Cursor()
			var toDelete [][]byte
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
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
	a.db.compositeIndexes.Delete(def.Key)
	return nil
}
