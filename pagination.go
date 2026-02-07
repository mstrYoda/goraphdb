package graphdb

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// NodePage holds a page of nodes returned by cursor-based pagination.
type NodePage struct {
	Nodes      []*Node `json:"nodes"`
	NextCursor NodeID  `json:"next_cursor"` // 0 means no more pages
	HasMore    bool    `json:"has_more"`
}

// EdgePage holds a page of edges returned by cursor-based pagination.
type EdgePage struct {
	Edges      []*Edge `json:"edges"`
	NextCursor EdgeID  `json:"next_cursor"` // 0 means no more pages
	HasMore    bool    `json:"has_more"`
}

// ListNodes returns a page of nodes using cursor-based pagination.
// Pass afterID=0 for the first page. The returned NextCursor should be passed
// as afterID for the next page. This is O(limit) per call — no offset scanning.
func (db *DB) ListNodes(afterID NodeID, limit int) (*NodePage, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if limit <= 0 {
		limit = 50
	}

	page := &NodePage{
		Nodes: make([]*Node, 0, limit),
	}

	for _, s := range db.shards {
		if len(page.Nodes) >= limit+1 {
			break
		}
		err := s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketNodes)
			c := b.Cursor()

			var k, v []byte
			if afterID == 0 {
				k, v = c.First()
			} else {
				// Seek to afterID+1 to skip the cursor node itself.
				seekKey := encodeNodeID(afterID + 1)
				k, v = c.Seek(seekKey)
			}

			for ; k != nil && len(page.Nodes) < limit+1; k, v = c.Next() {
				id := decodeNodeID(k)
				props, err := decodeProps(v)
				if err != nil {
					continue
				}
				labels := loadLabels(tx, id)
				page.Nodes = append(page.Nodes, &Node{ID: id, Labels: labels, Props: props})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// If we got limit+1 nodes, there are more pages.
	if len(page.Nodes) > limit {
		page.HasMore = true
		page.Nodes = page.Nodes[:limit]
	}

	if len(page.Nodes) > 0 {
		page.NextCursor = page.Nodes[len(page.Nodes)-1].ID
	}

	return page, nil
}

// ListEdges returns a page of edges using cursor-based pagination.
// Pass afterID=0 for the first page.
func (db *DB) ListEdges(afterID EdgeID, limit int) (*EdgePage, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if limit <= 0 {
		limit = 50
	}

	page := &EdgePage{
		Edges: make([]*Edge, 0, limit),
	}

	for _, s := range db.shards {
		if len(page.Edges) >= limit+1 {
			break
		}
		err := s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketEdges)
			c := b.Cursor()

			var k, v []byte
			if afterID == 0 {
				k, v = c.First()
			} else {
				seekKey := encodeEdgeID(afterID + 1)
				k, v = c.Seek(seekKey)
			}

			for ; k != nil && len(page.Edges) < limit+1; k, v = c.Next() {
				id := decodeEdgeID(k)
				edge, err := decodeEdge(v)
				if err != nil {
					continue
				}
				edge.ID = id
				page.Edges = append(page.Edges, edge)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if len(page.Edges) > limit {
		page.HasMore = true
		page.Edges = page.Edges[:limit]
	}

	if len(page.Edges) > 0 {
		page.NextCursor = page.Edges[len(page.Edges)-1].ID
	}

	return page, nil
}

// ListNodesByLabel returns a page of nodes with the given label using cursor pagination.
func (db *DB) ListNodesByLabel(label string, afterID NodeID, limit int) (*NodePage, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}
	if limit <= 0 {
		limit = 50
	}

	page := &NodePage{
		Nodes: make([]*Node, 0, limit),
	}

	prefix := encodeIndexPrefix(label)

	for _, s := range db.shards {
		if len(page.Nodes) >= limit+1 {
			break
		}
		err := s.db.View(func(tx *bolt.Tx) error {
			idxBucket := tx.Bucket(bucketIdxNodeLabel)
			nodesBucket := tx.Bucket(bucketNodes)
			c := idxBucket.Cursor()

			for k, _ := c.Seek(prefix); k != nil && len(k) >= len(prefix); k, _ = c.Next() {
				// Check prefix match.
				match := true
				for i := 0; i < len(prefix); i++ {
					if k[i] != prefix[i] {
						match = false
						break
					}
				}
				if !match {
					break
				}

				if len(k) < len(prefix)+8 {
					continue
				}
				nodeID := decodeNodeID(k[len(prefix):])

				// Skip nodes at or before cursor.
				if afterID > 0 && nodeID <= afterID {
					continue
				}

				if len(page.Nodes) >= limit+1 {
					break
				}

				data := nodesBucket.Get(encodeNodeID(nodeID))
				if data == nil {
					continue
				}
				props, err := decodeProps(data)
				if err != nil {
					continue
				}
				labels := loadLabels(tx, nodeID)
				page.Nodes = append(page.Nodes, &Node{ID: nodeID, Labels: labels, Props: props})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if len(page.Nodes) > limit {
		page.HasMore = true
		page.Nodes = page.Nodes[:limit]
	}

	if len(page.Nodes) > 0 {
		page.NextCursor = page.Nodes[len(page.Nodes)-1].ID
	}

	return page, nil
}
