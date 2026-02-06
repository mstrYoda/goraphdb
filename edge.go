package graphdb

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// AddEdge creates a directed, labeled edge from one node to another.
// Example: AddEdge(alice, bob, "follows", Props{"since": "2024"})
//
//	creates: alice ---follows---> bob
//
// Sharding layout:
//   - Edge data + adj_out + edge-type index → source node's shard (for fast outgoing traversals)
//   - adj_in                                → target node's shard (for fast incoming traversals)
//
// This means OutEdges(X) reads ONLY X's shard, InEdges(X) reads ONLY X's shard.
// Safe for concurrent use (serialized per-shard by bbolt).
func (db *DB) AddEdge(from, to NodeID, label string, props Props) (EdgeID, error) {
	if db.isClosed() {
		return 0, fmt.Errorf("graphdb: database is closed")
	}

	// Verify both nodes exist.
	if err := db.verifyNodeExists(from); err != nil {
		return 0, fmt.Errorf("graphdb: source node: %w", err)
	}
	if err := db.verifyNodeExists(to); err != nil {
		return 0, fmt.Errorf("graphdb: target node: %w", err)
	}

	// Allocate edge ID from primary shard.
	id := db.primaryShard().allocEdgeID()

	edge := &Edge{
		ID:    id,
		From:  from,
		To:    to,
		Label: label,
		Props: props,
	}

	srcShard := db.shardForEdge(from)
	dstShard := db.shardFor(to)

	if srcShard == dstShard {
		// Same shard: single transaction = single fsync.
		err := srcShard.db.Update(func(tx *bolt.Tx) error {
			edgeData, err := encodeEdge(edge)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bucketEdges).Put(encodeEdgeID(id), edgeData); err != nil {
				return err
			}

			// Outgoing adjacency: from -> to.
			if err := tx.Bucket(bucketAdjOut).Put(
				encodeAdjKey(from, id), encodeAdjValue(to, label),
			); err != nil {
				return err
			}

			// Incoming adjacency: to -> from (same shard, same tx).
			if err := tx.Bucket(bucketAdjIn).Put(
				encodeAdjKey(to, id), encodeAdjValue(from, label),
			); err != nil {
				return err
			}

			// Edge type index.
			if err := tx.Bucket(bucketIdxEdgeTyp).Put(
				encodeIndexKey(label, uint64(id)), nil,
			); err != nil {
				return err
			}

			srcShard.edgeCount.Add(1)
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("graphdb: failed to add edge: %w", err)
		}
		return id, nil
	}

	// Different shards: two transactions (one fsync each).
	// 1) Edge data + adj_out + index in source shard.
	err := srcShard.db.Update(func(tx *bolt.Tx) error {
		edgeData, err := encodeEdge(edge)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketEdges).Put(encodeEdgeID(id), edgeData); err != nil {
			return err
		}
		if err := tx.Bucket(bucketAdjOut).Put(
			encodeAdjKey(from, id), encodeAdjValue(to, label),
		); err != nil {
			return err
		}
		if err := tx.Bucket(bucketIdxEdgeTyp).Put(
			encodeIndexKey(label, uint64(id)), nil,
		); err != nil {
			return err
		}
		srcShard.edgeCount.Add(1)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("graphdb: failed to add edge (source shard): %w", err)
	}

	// 2) adj_in in target shard.
	err = dstShard.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAdjIn).Put(
			encodeAdjKey(to, id), encodeAdjValue(from, label),
		)
	})
	if err != nil {
		return 0, fmt.Errorf("graphdb: failed to add edge (target shard adj_in): %w", err)
	}

	return id, nil
}

// AddEdgeBatch creates multiple edges in a single transaction.
// All edges in single-shard mode go into one transaction for atomicity.
func (db *DB) AddEdgeBatch(edges []Edge) ([]EdgeID, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	ids := make([]EdgeID, len(edges))

	if len(db.shards) == 1 {
		// Single-shard fast path: everything in one transaction.
		s := db.shards[0]
		err := s.db.Update(func(tx *bolt.Tx) error {
			edgeBucket := tx.Bucket(bucketEdges)
			adjOutBucket := tx.Bucket(bucketAdjOut)
			adjInBucket := tx.Bucket(bucketAdjIn)
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)

			for i := range edges {
				id := s.allocEdgeID()
				ids[i] = id

				e := &edges[i]
				e.ID = id

				edgeData, err := encodeEdge(e)
				if err != nil {
					return err
				}
				if err := edgeBucket.Put(encodeEdgeID(id), edgeData); err != nil {
					return err
				}

				// Outgoing adjacency.
				if err := adjOutBucket.Put(
					encodeAdjKey(e.From, id),
					encodeAdjValue(e.To, e.Label),
				); err != nil {
					return err
				}
				// Incoming adjacency (same shard).
				if err := adjInBucket.Put(
					encodeAdjKey(e.To, id),
					encodeAdjValue(e.From, e.Label),
				); err != nil {
					return err
				}

				// Edge type index.
				if err := idxBucket.Put(encodeIndexKey(e.Label, uint64(id)), nil); err != nil {
					return err
				}
			}

			s.edgeCount.Add(uint64(len(edges)))
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("graphdb: batch edge add failed: %w", err)
		}
		return ids, nil
	}

	// Multi-shard: group edges by source/destination shard for batched transactions.
	// This avoids the O(N * fsync) cost of calling AddEdge individually.

	type adjInEntry struct {
		to     NodeID
		edgeID EdgeID
		from   NodeID
		label  string
	}
	srcGroups := make(map[*shard][]*Edge)
	dstGroups := make(map[*shard][]adjInEntry)

	for i := range edges {
		id := db.primaryShard().allocEdgeID()
		ids[i] = id
		edges[i].ID = id

		srcShard := db.shardForEdge(edges[i].From)
		dstShard := db.shardFor(edges[i].To)

		srcGroups[srcShard] = append(srcGroups[srcShard], &edges[i])
		dstGroups[dstShard] = append(dstGroups[dstShard], adjInEntry{
			to: edges[i].To, edgeID: id, from: edges[i].From, label: edges[i].Label,
		})
	}

	// Step 1: write edge data + adj_out + edge-type index per source shard.
	for s, batch := range srcGroups {
		err := s.db.Update(func(tx *bolt.Tx) error {
			edgeBucket := tx.Bucket(bucketEdges)
			adjOutBucket := tx.Bucket(bucketAdjOut)
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)

			for _, e := range batch {
				edgeData, err := encodeEdge(e)
				if err != nil {
					return err
				}
				if err := edgeBucket.Put(encodeEdgeID(e.ID), edgeData); err != nil {
					return err
				}
				if err := adjOutBucket.Put(
					encodeAdjKey(e.From, e.ID), encodeAdjValue(e.To, e.Label),
				); err != nil {
					return err
				}
				if err := idxBucket.Put(encodeIndexKey(e.Label, uint64(e.ID)), nil); err != nil {
					return err
				}
			}
			s.edgeCount.Add(uint64(len(batch)))
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("graphdb: batch edge add failed: %w", err)
		}
	}

	// Step 2: write adj_in entries per destination shard.
	for s, batch := range dstGroups {
		err := s.db.Update(func(tx *bolt.Tx) error {
			adjInBucket := tx.Bucket(bucketAdjIn)
			for _, entry := range batch {
				if err := adjInBucket.Put(
					encodeAdjKey(entry.to, entry.edgeID),
					encodeAdjValue(entry.from, entry.label),
				); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("graphdb: batch edge add failed (adj_in): %w", err)
		}
	}

	return ids, nil
}

// GetEdge retrieves an edge by its ID. Safe for concurrent use.
func (db *DB) GetEdge(id EdgeID) (*Edge, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	return db.getEdge(id)
}

// getEdge is the lock-free internal version.
// Edge data lives in the source node's shard, but we don't know the source from just an EdgeID,
// so we must scan shards. For single-shard mode this is a single lookup.
func (db *DB) getEdge(id EdgeID) (*Edge, error) {
	for _, s := range db.shards {
		var edge *Edge
		err := s.db.View(func(tx *bolt.Tx) error {
			data := tx.Bucket(bucketEdges).Get(encodeEdgeID(id))
			if data == nil {
				return nil
			}
			var err error
			edge, err = decodeEdge(data)
			return err
		})
		if err != nil {
			return nil, err
		}
		if edge != nil {
			return edge, nil
		}
	}
	return nil, fmt.Errorf("graphdb: edge %d not found", id)
}

// DeleteEdge removes an edge and its adjacency entries from both shards.
func (db *DB) DeleteEdge(id EdgeID) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}

	// Find the edge first.
	edge, err := db.getEdge(id)
	if err != nil {
		return err
	}

	return db.deleteEdgeInternal(edge)
}

// deleteEdgeInternal removes an edge from both source and target shards.
func (db *DB) deleteEdgeInternal(edge *Edge) error {
	srcShard := db.shardForEdge(edge.From)
	dstShard := db.shardFor(edge.To)

	// 1) Remove edge data + adj_out + edge-type index from source shard.
	err := srcShard.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketEdges).Delete(encodeEdgeID(edge.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketAdjOut).Delete(encodeAdjKey(edge.From, edge.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketIdxEdgeTyp).Delete(encodeIndexKey(edge.Label, uint64(edge.ID))); err != nil {
			return err
		}
		srcShard.edgeCount.Add(^uint64(0)) // decrement
		return nil
	})
	if err != nil {
		return err
	}

	// 2) Remove adj_in from target shard.
	if dstShard == srcShard {
		return srcShard.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketAdjIn).Delete(encodeAdjKey(edge.To, edge.ID))
		})
	}
	return dstShard.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAdjIn).Delete(encodeAdjKey(edge.To, edge.ID))
	})
}

// UpdateEdge updates the properties of an existing edge (merge).
func (db *DB) UpdateEdge(id EdgeID, props Props) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}

	edge, err := db.getEdge(id)
	if err != nil {
		return err
	}

	// Merge props.
	if edge.Props == nil {
		edge.Props = make(Props)
	}
	for k, v := range props {
		edge.Props[k] = v
	}

	s := db.shardForEdge(edge.From)
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := encodeEdge(edge)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketEdges).Put(encodeEdgeID(id), data)
	})
}

// OutEdges returns all outgoing edges from a node. Safe for concurrent use.
// In sharded mode, reads ONLY the node's own shard — O(1) shard lookups.
func (db *DB) OutEdges(id NodeID) ([]*Edge, error) {
	return db.getEdgesForNode(id, Outgoing)
}

// InEdges returns all incoming edges to a node. Safe for concurrent use.
// In sharded mode, reads ONLY the node's own shard — O(1) shard lookups.
func (db *DB) InEdges(id NodeID) ([]*Edge, error) {
	return db.getEdgesForNode(id, Incoming)
}

// Edges returns all edges connected to a node (both directions).
func (db *DB) Edges(id NodeID) ([]*Edge, error) {
	return db.getEdgesForNode(id, Both)
}

// OutEdgesLabeled returns outgoing edges with a specific label.
// Example: OutEdgesLabeled(alice, "follows") returns all "follows" edges from alice.
func (db *DB) OutEdgesLabeled(id NodeID, label string) ([]*Edge, error) {
	edges, err := db.getEdgesForNode(id, Outgoing)
	if err != nil {
		return nil, err
	}
	var filtered []*Edge
	for _, e := range edges {
		if e.Label == label {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

// InEdgesLabeled returns incoming edges with a specific label.
func (db *DB) InEdgesLabeled(id NodeID, label string) ([]*Edge, error) {
	edges, err := db.getEdgesForNode(id, Incoming)
	if err != nil {
		return nil, err
	}
	var filtered []*Edge
	for _, e := range edges {
		if e.Label == label {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

// Neighbors returns all nodes connected to the given node by outgoing edges.
func (db *DB) Neighbors(id NodeID) ([]*Node, error) {
	return db.NeighborsDirection(id, Outgoing)
}

// NeighborsLabeled returns neighbors connected by edges with a specific label.
func (db *DB) NeighborsLabeled(id NodeID, label string) ([]*Node, error) {
	edges, err := db.OutEdgesLabeled(id, label)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(edges))
	for _, e := range edges {
		n, err := db.getNode(e.To)
		if err != nil {
			continue // skip missing nodes (may have been concurrently deleted)
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// NeighborsDirection returns nodes connected in the given direction.
func (db *DB) NeighborsDirection(id NodeID, dir Direction) ([]*Node, error) {
	edges, err := db.getEdgesForNode(id, dir)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(edges))
	seen := make(map[NodeID]bool)
	for _, e := range edges {
		targetID := e.To
		if dir == Incoming {
			targetID = e.From
		}
		if seen[targetID] {
			continue
		}
		seen[targetID] = true
		n, err := db.getNode(targetID)
		if err != nil {
			continue
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// Degree returns the number of edges connected to a node in the given direction.
func (db *DB) Degree(id NodeID, dir Direction) (int, error) {
	edges, err := db.getEdgesForNode(id, dir)
	if err != nil {
		return 0, err
	}
	return len(edges), nil
}

// EdgeCount returns the total number of edges in the database.
func (db *DB) EdgeCount() uint64 {
	var total uint64
	for _, s := range db.shards {
		total += s.edgeCount.Load()
	}
	return total
}

// HasEdge checks if a direct edge exists between two nodes.
func (db *DB) HasEdge(from, to NodeID) (bool, error) {
	edges, err := db.getEdgesForNode(from, Outgoing)
	if err != nil {
		return false, err
	}
	for _, e := range edges {
		if e.To == to {
			return true, nil
		}
	}
	return false, nil
}

// HasEdgeLabeled checks if a direct edge with specific label exists between two nodes.
func (db *DB) HasEdgeLabeled(from, to NodeID, label string) (bool, error) {
	edges, err := db.OutEdgesLabeled(from, label)
	if err != nil {
		return false, err
	}
	for _, e := range edges {
		if e.To == to {
			return true, nil
		}
	}
	return false, nil
}

// EdgesByLabel returns all edges with the given label.
// Note: this must scan all shards since edges are distributed by source node.
func (db *DB) EdgesByLabel(label string) ([]*Edge, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	var edges []*Edge
	prefix := encodeIndexPrefix(label)

	for _, s := range db.shards {
		err := s.db.View(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxEdgeTyp)
			edgeBucket := tx.Bucket(bucketEdges)

			c := idxBucket.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				edgeIDBytes := k[len(prefix):]
				if len(edgeIDBytes) < 8 {
					continue
				}
				edgeID := decodeEdgeID(edgeIDBytes)

				data := edgeBucket.Get(encodeEdgeID(edgeID))
				if data == nil {
					continue
				}
				edge, err := decodeEdge(data)
				if err != nil {
					continue
				}
				edges = append(edges, edge)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return edges, nil
}

// verifyNodeExists checks if a node exists.
func (db *DB) verifyNodeExists(id NodeID) error {
	s := db.shardFor(id)
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketNodes).Get(encodeNodeID(id)) == nil {
			return fmt.Errorf("node %d not found", id)
		}
		return nil
	})
}

// getEdgesForNode retrieves edges for a node in the given direction.
//
// Shard-aware routing:
//   - Outgoing: reads adj_out from the node's own shard only (edge data is co-located)
//   - Incoming: reads adj_in from the node's own shard only (adj_in stored in target's shard)
//   - Both:     reads adj_out + adj_in from the node's own shard
//
// This means every OutEdges / InEdges / Neighbors call hits exactly 1 shard.
// The only exception is that for Incoming edges, we need to fetch the full Edge data
// from the source shard (since edge data lives with the source node).
func (db *DB) getEdgesForNode(id NodeID, dir Direction) ([]*Edge, error) {
	s := db.shardFor(id)
	prefix := encodeNodeID(id)
	var edges []*Edge

	// collectLocal reads adjacency entries + edge data from the SAME shard.
	// Works for adj_out because edge data is co-located with the source node.
	collectLocal := func(bucketName []byte) error {
		return s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			edgeBucket := tx.Bucket(bucketEdges)

			c := b.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				_, edgeID := decodeAdjKey(k)
				data := edgeBucket.Get(encodeEdgeID(edgeID))
				if data == nil {
					continue
				}
				edge, err := decodeEdge(data)
				if err != nil {
					continue
				}
				edges = append(edges, edge)
			}
			return nil
		})
	}

	// collectIncoming reads adj_in entries from the node's shard,
	// then fetches full Edge data from each edge's source shard.
	collectIncoming := func() error {
		// Step 1: read adj_in entries from this node's shard to get edge IDs + source info.
		type adjEntry struct {
			edgeID   EdgeID
			sourceID NodeID
			label    string
		}
		var entries []adjEntry

		err := s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketAdjIn)
			c := b.Cursor()
			for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
				_, edgeID := decodeAdjKey(k)
				sourceID, label := decodeAdjValue(v)
				entries = append(entries, adjEntry{edgeID: edgeID, sourceID: sourceID, label: label})
			}
			return nil
		})
		if err != nil {
			return err
		}

		// Step 2: fetch full edge data from each source shard.
		// Group by shard to minimize transactions.
		type shardGroup struct {
			shard   *shard
			edgeIDs []EdgeID
		}
		groups := make(map[*shard]*shardGroup)
		for _, e := range entries {
			srcShard := db.shardForEdge(e.sourceID)
			g, ok := groups[srcShard]
			if !ok {
				g = &shardGroup{shard: srcShard}
				groups[srcShard] = g
			}
			g.edgeIDs = append(g.edgeIDs, e.edgeID)
		}

		for _, g := range groups {
			err := g.shard.db.View(func(tx *bolt.Tx) error {
				edgeBucket := tx.Bucket(bucketEdges)
				for _, eid := range g.edgeIDs {
					data := edgeBucket.Get(encodeEdgeID(eid))
					if data == nil {
						continue
					}
					edge, err := decodeEdge(data)
					if err != nil {
						continue
					}
					edges = append(edges, edge)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	}

	if dir == Outgoing || dir == Both {
		// adj_out + edge data are in the same shard — single shard read.
		if err := collectLocal(bucketAdjOut); err != nil {
			return nil, err
		}
	}

	if dir == Incoming || dir == Both {
		if len(db.shards) == 1 {
			// Single shard: adj_in + edge data are co-located.
			if err := collectLocal(bucketAdjIn); err != nil {
				return nil, err
			}
		} else {
			// Multi shard: adj_in is local, but edge data may be in another shard.
			if err := collectIncoming(); err != nil {
				return nil, err
			}
		}
	}

	return edges, nil
}
