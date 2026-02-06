package graphdb

import (
	"container/heap"
	"container/list"
	"fmt"
)

// ShortestPath finds the shortest path between two nodes using BFS (unweighted).
// Returns nil if no path exists. Safe for concurrent use.
func (db *DB) ShortestPath(fromID, toID NodeID) (*PathResult, error) {
	return db.ShortestPathLabeled(fromID, toID, "")
}

// ShortestPathLabeled finds the shortest path following only edges with the given label.
// Pass empty string for label to follow all edges.
func (db *DB) ShortestPathLabeled(fromID, toID NodeID, label string) (*PathResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	if fromID == toID {
		node, err := db.getNode(fromID)
		if err != nil {
			return nil, err
		}
		return &PathResult{
			Nodes: []*Node{node},
			Edges: nil,
			Cost:  0,
		}, nil
	}

	// BFS for shortest path in unweighted graph.
	type bfsEntry struct {
		nodeID NodeID
		path   []*Edge
	}

	visited := make(map[NodeID]bool)
	visited[fromID] = true

	queue := list.New()
	queue.PushBack(bfsEntry{nodeID: fromID, path: nil})

	for queue.Len() > 0 {
		front := queue.Front()
		queue.Remove(front)
		entry := front.Value.(bfsEntry)

		edges, err := db.getEdgesForNode(entry.nodeID, Outgoing)
		if err != nil {
			return nil, err
		}

		for _, edge := range edges {
			if label != "" && edge.Label != label {
				continue
			}

			if visited[edge.To] {
				continue
			}
			visited[edge.To] = true

			newPath := make([]*Edge, len(entry.path)+1)
			copy(newPath, entry.path)
			newPath[len(entry.path)] = edge

			if edge.To == toID {
				return db.buildPathResult(fromID, newPath)
			}

			queue.PushBack(bfsEntry{nodeID: edge.To, path: newPath})
		}
	}

	return nil, nil // no path found
}

// ShortestPathWeighted finds the shortest path using Dijkstra's algorithm.
// The weightProp parameter specifies which edge property to use as weight.
// If an edge doesn't have the weight property, defaultWeight is used.
func (db *DB) ShortestPathWeighted(fromID, toID NodeID, weightProp string, defaultWeight float64) (*PathResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	if fromID == toID {
		node, err := db.getNode(fromID)
		if err != nil {
			return nil, err
		}
		return &PathResult{
			Nodes: []*Node{node},
			Edges: nil,
			Cost:  0,
		}, nil
	}

	// Dijkstra's algorithm.
	dist := make(map[NodeID]float64)
	prev := make(map[NodeID]NodeID)
	prevEdge := make(map[NodeID]*Edge)

	dist[fromID] = 0

	pq := &priorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &pqItem{nodeID: fromID, cost: 0})

	for pq.Len() > 0 {
		item := heap.Pop(pq).(*pqItem)
		current := item.nodeID

		if current == toID {
			break
		}

		if item.cost > dist[current] {
			continue // outdated entry
		}

		edges, err := db.getEdgesForNode(current, Outgoing)
		if err != nil {
			return nil, err
		}

		for _, edge := range edges {
			weight := defaultWeight
			if w, ok := edge.Props[weightProp]; ok {
				switch v := w.(type) {
				case float64:
					weight = v
				case int:
					weight = float64(v)
				case int64:
					weight = float64(v)
				}
			}

			newDist := dist[current] + weight
			if existing, ok := dist[edge.To]; !ok || newDist < existing {
				dist[edge.To] = newDist
				prev[edge.To] = current
				prevEdge[edge.To] = edge
				heap.Push(pq, &pqItem{nodeID: edge.To, cost: newDist})
			}
		}
	}

	// Reconstruct path.
	if _, ok := dist[toID]; !ok {
		return nil, nil // no path found
	}

	var pathEdges []*Edge
	current := toID
	for current != fromID {
		edge, ok := prevEdge[current]
		if !ok {
			return nil, nil
		}
		pathEdges = append([]*Edge{edge}, pathEdges...)
		current = prev[current]
	}

	return db.buildPathResult(fromID, pathEdges)
}

// AllPaths finds all paths between two nodes up to a maximum depth.
// Warning: Can be expensive for dense graphs. Use maxDepth to limit.
func (db *DB) AllPaths(fromID, toID NodeID, maxDepth int) ([]*PathResult, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	if maxDepth <= 0 {
		maxDepth = 10 // reasonable default
	}

	var results []*PathResult

	// DFS-based path enumeration.
	var dfs func(current NodeID, path []*Edge, visited map[NodeID]bool) error
	dfs = func(current NodeID, path []*Edge, visited map[NodeID]bool) error {
		if current == toID {
			result, err := db.buildPathResult(fromID, path)
			if err != nil {
				return err
			}
			results = append(results, result)
			return nil
		}

		if len(path) >= maxDepth {
			return nil
		}

		edges, err := db.getEdgesForNode(current, Outgoing)
		if err != nil {
			return err
		}

		for _, edge := range edges {
			if visited[edge.To] {
				continue
			}

			visited[edge.To] = true
			newPath := make([]*Edge, len(path)+1)
			copy(newPath, path)
			newPath[len(path)] = edge

			if err := dfs(edge.To, newPath, visited); err != nil {
				return err
			}
			delete(visited, edge.To)
		}
		return nil
	}

	visited := map[NodeID]bool{fromID: true}
	err := dfs(fromID, nil, visited)
	return results, err
}

// HasPath checks if any path exists between two nodes.
func (db *DB) HasPath(fromID, toID NodeID) (bool, error) {
	path, err := db.ShortestPath(fromID, toID)
	if err != nil {
		return false, err
	}
	return path != nil, nil
}

// ConnectedComponents returns groups of nodes that are connected.
// Uses BFS to discover each component.
func (db *DB) ConnectedComponents() ([][]NodeID, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	visited := make(map[NodeID]bool)
	var components [][]NodeID

	// Iterate over all nodes.
	err := db.forEachNode(func(node *Node) error {
		if visited[node.ID] {
			return nil
		}

		// BFS from this node to find all connected nodes.
		var component []NodeID
		queue := list.New()
		queue.PushBack(node.ID)
		visited[node.ID] = true

		for queue.Len() > 0 {
			front := queue.Front()
			queue.Remove(front)
			currentID := front.Value.(NodeID)
			component = append(component, currentID)

			// Follow both directions for undirected connectivity.
			edges, err := db.getEdgesForNode(currentID, Both)
			if err != nil {
				return err
			}

			for _, edge := range edges {
				targetID := edge.To
				if targetID == currentID {
					targetID = edge.From
				}
				if !visited[targetID] {
					visited[targetID] = true
					queue.PushBack(targetID)
				}
			}
		}

		components = append(components, component)
		return nil
	})

	return components, err
}

// TopologicalSort returns nodes in topological order (for DAGs).
// Returns error if the graph contains cycles.
func (db *DB) TopologicalSort() ([]NodeID, error) {
	if db.isClosed() {
		return nil, fmt.Errorf("graphdb: database is closed")
	}

	// Kahn's algorithm.
	inDegree := make(map[NodeID]int)
	allNodes := make(map[NodeID]bool)

	// Compute in-degrees.
	err := db.forEachNode(func(node *Node) error {
		allNodes[node.ID] = true
		if _, exists := inDegree[node.ID]; !exists {
			inDegree[node.ID] = 0
		}

		edges, err := db.getEdgesForNode(node.ID, Outgoing)
		if err != nil {
			return err
		}
		for _, e := range edges {
			inDegree[e.To]++
			allNodes[e.To] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Start with nodes that have no incoming edges.
	queue := list.New()
	for id := range allNodes {
		if inDegree[id] == 0 {
			queue.PushBack(id)
		}
	}

	var sorted []NodeID
	for queue.Len() > 0 {
		front := queue.Front()
		queue.Remove(front)
		nodeID := front.Value.(NodeID)
		sorted = append(sorted, nodeID)

		edges, err := db.getEdgesForNode(nodeID, Outgoing)
		if err != nil {
			return nil, err
		}

		for _, e := range edges {
			inDegree[e.To]--
			if inDegree[e.To] == 0 {
				queue.PushBack(e.To)
			}
		}
	}

	if len(sorted) != len(allNodes) {
		return nil, fmt.Errorf("graphdb: graph contains cycles, topological sort not possible")
	}

	return sorted, nil
}

// buildPathResult constructs a PathResult from a starting node and edge list.
func (db *DB) buildPathResult(fromID NodeID, pathEdges []*Edge) (*PathResult, error) {
	result := &PathResult{
		Edges: pathEdges,
		Cost:  float64(len(pathEdges)),
	}

	// Collect all nodes along the path.
	nodeIDs := make([]NodeID, 0, len(pathEdges)+1)
	nodeIDs = append(nodeIDs, fromID)
	for _, e := range pathEdges {
		nodeIDs = append(nodeIDs, e.To)
	}

	result.Nodes = make([]*Node, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		node, err := db.getNode(id)
		if err != nil {
			return nil, err
		}
		result.Nodes = append(result.Nodes, node)
	}

	return result, nil
}

// --- Priority Queue for Dijkstra ---

type pqItem struct {
	nodeID NodeID
	cost   float64
	index  int
}

type priorityQueue []*pqItem

func (pq priorityQueue) Len() int          { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].cost < pq[j].cost }
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[:n-1]
	return item
}
