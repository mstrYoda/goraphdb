package graphdb

import (
	"container/list"
	"fmt"
)

// BFS performs a breadth-first search starting from the given node.
// It visits nodes level by level, calling the visitor for each discovered node.
//
// **Fully concurrent** — multiple BFS/DFS/Query calls can run simultaneously
// from different goroutines without blocking each other.
//
// Parameters:
//   - startID: the starting node
//   - maxDepth: maximum traversal depth (-1 for unlimited)
//   - direction: which edges to follow (Outgoing, Incoming, Both)
//   - edgeFilter: optional filter to select which edges to traverse (nil = all edges)
//   - visitor: called for each discovered node; return false to stop traversal
//
// Example:
//
//	db.BFS(aliceID, 3, Outgoing, nil, func(r *TraversalResult) bool {
//	    fmt.Printf("Found %v at depth %d\n", r.Node.Props["name"], r.Depth)
//	    return true // continue
//	})
func (db *DB) BFS(startID NodeID, maxDepth int, direction Direction, edgeFilter EdgeFilter, visitor Visitor) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}

	startNode, err := db.getNode(startID)
	if err != nil {
		return err
	}

	// visited tracks which nodes we've already seen.
	visited := make(map[NodeID]bool)
	visited[startID] = true

	// pathTo stores the edge path to each visited node.
	pathTo := make(map[NodeID][]*Edge)
	pathTo[startID] = nil

	// Visit the start node at depth 0.
	if !visitor(&TraversalResult{Node: startNode, Depth: 0, Path: nil}) {
		return nil
	}

	// BFS queue entries.
	type queueEntry struct {
		nodeID NodeID
		depth  int
	}

	queue := list.New()
	queue.PushBack(queueEntry{nodeID: startID, depth: 0})

	for queue.Len() > 0 {
		front := queue.Front()
		queue.Remove(front)
		entry := front.Value.(queueEntry)

		// Check depth limit.
		if maxDepth >= 0 && entry.depth >= maxDepth {
			continue
		}

		// Get edges based on direction.
		edges, err := db.getEdgesForNode(entry.nodeID, direction)
		if err != nil {
			return err
		}

		for _, edge := range edges {
			// Apply edge filter.
			if edgeFilter != nil && !edgeFilter(edge) {
				continue
			}

			// Determine the target node based on direction.
			targetID := edge.To
			if direction == Incoming {
				targetID = edge.From
			} else if direction == Both {
				targetID = edge.To
				if edge.To == entry.nodeID {
					targetID = edge.From
				}
			}

			if visited[targetID] {
				continue
			}
			visited[targetID] = true

			// Build the path to this node.
			parentPath := pathTo[entry.nodeID]
			edgePath := make([]*Edge, len(parentPath)+1)
			copy(edgePath, parentPath)
			edgePath[len(parentPath)] = edge
			pathTo[targetID] = edgePath

			// Get the target node.
			targetNode, err := db.getNode(targetID)
			if err != nil {
				continue // node may have been deleted concurrently
			}

			newDepth := entry.depth + 1
			result := &TraversalResult{
				Node:  targetNode,
				Depth: newDepth,
				Path:  edgePath,
			}

			if !visitor(result) {
				return nil // visitor requested stop
			}

			queue.PushBack(queueEntry{nodeID: targetID, depth: newDepth})
		}
	}

	return nil
}

// DFS performs a depth-first search starting from the given node.
// It explores as deep as possible along each branch before backtracking.
//
// **Fully concurrent** — multiple BFS/DFS/Query calls can run simultaneously.
//
// Parameters:
//   - startID: the starting node
//   - maxDepth: maximum traversal depth (-1 for unlimited)
//   - direction: which edges to follow (Outgoing, Incoming, Both)
//   - edgeFilter: optional filter to select which edges to traverse (nil = all edges)
//   - visitor: called for each discovered node; return false to stop traversal
func (db *DB) DFS(startID NodeID, maxDepth int, direction Direction, edgeFilter EdgeFilter, visitor Visitor) error {
	if db.isClosed() {
		return fmt.Errorf("graphdb: database is closed")
	}

	startNode, err := db.getNode(startID)
	if err != nil {
		return err
	}

	visited := make(map[NodeID]bool)
	visited[startID] = true

	// Visit start node.
	if !visitor(&TraversalResult{Node: startNode, Depth: 0, Path: nil}) {
		return nil
	}

	// DFS stack entries.
	type stackEntry struct {
		nodeID NodeID
		depth  int
		path   []*Edge
	}

	stack := []stackEntry{{nodeID: startID, depth: 0, path: nil}}

	for len(stack) > 0 {
		// Pop from stack.
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Check depth limit.
		if maxDepth >= 0 && top.depth >= maxDepth {
			continue
		}

		// Get edges.
		edges, err := db.getEdgesForNode(top.nodeID, direction)
		if err != nil {
			return err
		}

		// Process edges in reverse order so that the first edge
		// is processed first (pushed last onto the stack).
		for i := len(edges) - 1; i >= 0; i-- {
			edge := edges[i]

			if edgeFilter != nil && !edgeFilter(edge) {
				continue
			}

			targetID := edge.To
			if direction == Incoming {
				targetID = edge.From
			} else if direction == Both {
				targetID = edge.To
				if edge.To == top.nodeID {
					targetID = edge.From
				}
			}

			if visited[targetID] {
				continue
			}
			visited[targetID] = true

			// Build path.
			edgePath := make([]*Edge, len(top.path)+1)
			copy(edgePath, top.path)
			edgePath[len(top.path)] = edge

			targetNode, err := db.getNode(targetID)
			if err != nil {
				continue
			}

			newDepth := top.depth + 1
			result := &TraversalResult{
				Node:  targetNode,
				Depth: newDepth,
				Path:  edgePath,
			}

			if !visitor(result) {
				return nil
			}

			stack = append(stack, stackEntry{
				nodeID: targetID,
				depth:  newDepth,
				path:   edgePath,
			})
		}
	}

	return nil
}

// BFSCollect performs BFS and returns all discovered nodes up to maxDepth.
// This is a convenience wrapper around BFS for simple use cases.
func (db *DB) BFSCollect(startID NodeID, maxDepth int, direction Direction) ([]*TraversalResult, error) {
	var results []*TraversalResult

	err := db.BFS(startID, maxDepth, direction, nil, func(r *TraversalResult) bool {
		results = append(results, r)
		return true
	})

	return results, err
}

// DFSCollect performs DFS and returns all discovered nodes up to maxDepth.
func (db *DB) DFSCollect(startID NodeID, maxDepth int, direction Direction) ([]*TraversalResult, error) {
	var results []*TraversalResult

	err := db.DFS(startID, maxDepth, direction, nil, func(r *TraversalResult) bool {
		results = append(results, r)
		return true
	})

	return results, err
}

// BFSFiltered performs BFS with both node and edge filters.
func (db *DB) BFSFiltered(startID NodeID, maxDepth int, direction Direction, edgeFilter EdgeFilter, nodeFilter NodeFilter) ([]*TraversalResult, error) {
	var results []*TraversalResult

	err := db.BFS(startID, maxDepth, direction, edgeFilter, func(r *TraversalResult) bool {
		if nodeFilter == nil || nodeFilter(r.Node) {
			results = append(results, r)
		}
		return true
	})

	return results, err
}

// DFSFiltered performs DFS with both node and edge filters.
func (db *DB) DFSFiltered(startID NodeID, maxDepth int, direction Direction, edgeFilter EdgeFilter, nodeFilter NodeFilter) ([]*TraversalResult, error) {
	var results []*TraversalResult

	err := db.DFS(startID, maxDepth, direction, edgeFilter, func(r *TraversalResult) bool {
		if nodeFilter == nil || nodeFilter(r.Node) {
			results = append(results, r)
		}
		return true
	})

	return results, err
}
