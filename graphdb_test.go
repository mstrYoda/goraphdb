package graphdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// testDB creates a temporary database for testing.
func testDB(t *testing.T, opts ...Options) *DB {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "testdb")

	opt := DefaultOptions()
	if len(opts) > 0 {
		opt = opts[0]
	}

	db, err := Open(dir, opt)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func TestOpenClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "testdb")

	db, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	stats, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.NodeCount != 0 || stats.EdgeCount != 0 {
		t.Errorf("expected empty db, got nodes=%d edges=%d", stats.NodeCount, stats.EdgeCount)
	}
	if stats.ShardCount != 1 {
		t.Errorf("expected 1 shard, got %d", stats.ShardCount)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen should work.
	db2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()
}

func TestNodeCRUD(t *testing.T) {
	db := testDB(t)

	// Add node.
	id, err := db.AddNode(Props{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}
	if id != 1 {
		t.Errorf("expected id=1, got %d", id)
	}

	// Get node.
	node, err := db.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	if node.GetString("name") != "Alice" {
		t.Errorf("expected name=Alice, got %s", node.GetString("name"))
	}

	// Update node (merge).
	err = db.UpdateNode(id, Props{"age": 31, "city": "Istanbul"})
	if err != nil {
		t.Fatalf("UpdateNode failed: %v", err)
	}

	node, _ = db.GetNode(id)
	if node.GetString("name") != "Alice" {
		t.Error("name should still be Alice after merge update")
	}
	if node.GetString("city") != "Istanbul" {
		t.Error("city should be Istanbul after update")
	}

	// Delete node.
	err = db.DeleteNode(id)
	if err != nil {
		t.Fatalf("DeleteNode failed: %v", err)
	}

	_, err = db.GetNode(id)
	if err == nil {
		t.Error("expected error getting deleted node")
	}

	if db.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", db.NodeCount())
	}
}

func TestEdgeCRUD(t *testing.T) {
	db := testDB(t)

	alice, _ := db.AddNode(Props{"name": "Alice"})
	bob, _ := db.AddNode(Props{"name": "Bob"})

	// Add edge: Alice ---follows---> Bob
	edgeID, err := db.AddEdge(alice, bob, "follows", Props{"since": "2024"})
	if err != nil {
		t.Fatalf("AddEdge failed: %v", err)
	}

	// Get edge.
	edge, err := db.GetEdge(edgeID)
	if err != nil {
		t.Fatalf("GetEdge failed: %v", err)
	}
	if edge.Label != "follows" {
		t.Errorf("expected label=follows, got %s", edge.Label)
	}
	if edge.From != alice || edge.To != bob {
		t.Errorf("expected from=%d to=%d, got from=%d to=%d", alice, bob, edge.From, edge.To)
	}

	// Out edges.
	outEdges, err := db.OutEdges(alice)
	if err != nil {
		t.Fatalf("OutEdges failed: %v", err)
	}
	if len(outEdges) != 1 {
		t.Fatalf("expected 1 out edge, got %d", len(outEdges))
	}

	// In edges.
	inEdges, err := db.InEdges(bob)
	if err != nil {
		t.Fatalf("InEdges failed: %v", err)
	}
	if len(inEdges) != 1 {
		t.Fatalf("expected 1 in edge, got %d", len(inEdges))
	}

	// Neighbors.
	neighbors, err := db.Neighbors(alice)
	if err != nil {
		t.Fatalf("Neighbors failed: %v", err)
	}
	if len(neighbors) != 1 || neighbors[0].ID != bob {
		t.Errorf("expected Bob as neighbor, got %v", neighbors)
	}

	// Has edge.
	has, _ := db.HasEdge(alice, bob)
	if !has {
		t.Error("expected HasEdge to return true")
	}

	// Delete edge.
	err = db.DeleteEdge(edgeID)
	if err != nil {
		t.Fatalf("DeleteEdge failed: %v", err)
	}
	if db.EdgeCount() != 0 {
		t.Errorf("expected 0 edges, got %d", db.EdgeCount())
	}
}

func TestEdgeLabeled(t *testing.T) {
	db := testDB(t)

	alice, _ := db.AddNode(Props{"name": "Alice"})
	bob, _ := db.AddNode(Props{"name": "Bob"})
	charlie, _ := db.AddNode(Props{"name": "Charlie"})
	topic, _ := db.AddNode(Props{"name": "GraphDB", "type": "topic"})

	// Alice follows Bob and Charlie.
	db.AddEdge(alice, bob, "follows", nil)
	db.AddEdge(alice, charlie, "follows", nil)

	// Alice consumes the topic.
	db.AddEdge(alice, topic, "consumes", nil)

	// Get only "follows" neighbors.
	follows, err := db.NeighborsLabeled(alice, "follows")
	if err != nil {
		t.Fatal(err)
	}
	if len(follows) != 2 {
		t.Errorf("expected 2 follows, got %d", len(follows))
	}

	// Get only "consumes" neighbors.
	consumes, err := db.NeighborsLabeled(alice, "consumes")
	if err != nil {
		t.Fatal(err)
	}
	if len(consumes) != 1 {
		t.Errorf("expected 1 consumes, got %d", len(consumes))
	}

	// EdgesByLabel.
	followEdges, err := db.EdgesByLabel("follows")
	if err != nil {
		t.Fatal(err)
	}
	if len(followEdges) != 2 {
		t.Errorf("expected 2 follow edges, got %d", len(followEdges))
	}
}

func TestBFS(t *testing.T) {
	db := testDB(t)

	// Build a graph:
	// A -> B -> C -> D
	//      |         ^
	//      +-> E ----+
	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})
	d, _ := db.AddNode(Props{"name": "D"})
	e, _ := db.AddNode(Props{"name": "E"})

	db.AddEdge(a, b, "connects", nil)
	db.AddEdge(b, c, "connects", nil)
	db.AddEdge(c, d, "connects", nil)
	db.AddEdge(b, e, "connects", nil)
	db.AddEdge(e, d, "connects", nil)

	// BFS from A with unlimited depth.
	results, err := db.BFSCollect(a, -1, Outgoing)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 5 { // A, B, C, E, D
		t.Errorf("expected 5 results, got %d", len(results))
	}

	// BFS with depth 1 should only find A and B.
	results, err = db.BFSCollect(a, 1, Outgoing)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results at depth 1, got %d", len(results))
	}

	// BFS with depth 2 should find A, B, C, E.
	results, err = db.BFSCollect(a, 2, Outgoing)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Errorf("expected 4 results at depth 2, got %d", len(results))
	}

	_ = d // used in edge setup
}

func TestDFS(t *testing.T) {
	db := testDB(t)

	// Build a tree:
	// A -> B -> D
	// A -> C -> E
	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})
	d, _ := db.AddNode(Props{"name": "D"})
	e, _ := db.AddNode(Props{"name": "E"})

	db.AddEdge(a, b, "connects", nil)
	db.AddEdge(a, c, "connects", nil)
	db.AddEdge(b, d, "connects", nil)
	db.AddEdge(c, e, "connects", nil)

	results, err := db.DFSCollect(a, -1, Outgoing)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}

	// First result should be the start node.
	if results[0].Node.ID != a {
		t.Error("first DFS result should be the start node")
	}
}

func TestShortestPath(t *testing.T) {
	db := testDB(t)

	// Build graph: A -> B -> C -> D
	//              A -> D (shortcut)
	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})
	d, _ := db.AddNode(Props{"name": "D"})

	db.AddEdge(a, b, "road", nil)
	db.AddEdge(b, c, "road", nil)
	db.AddEdge(c, d, "road", nil)
	db.AddEdge(a, d, "highway", nil) // shortcut

	path, err := db.ShortestPath(a, d)
	if err != nil {
		t.Fatal(err)
	}
	if path == nil {
		t.Fatal("expected a path, got nil")
	}

	// Shortest path should be A -> D (1 hop via highway).
	if len(path.Edges) != 1 {
		t.Errorf("expected 1-hop path, got %d hops", len(path.Edges))
	}
	if path.Edges[0].Label != "highway" {
		t.Errorf("expected highway edge, got %s", path.Edges[0].Label)
	}
}

func TestAllPaths(t *testing.T) {
	db := testDB(t)

	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})
	d, _ := db.AddNode(Props{"name": "D"})

	db.AddEdge(a, b, "road", nil)
	db.AddEdge(b, d, "road", nil)
	db.AddEdge(a, c, "road", nil)
	db.AddEdge(c, d, "road", nil)

	paths, err := db.AllPaths(a, d, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Errorf("expected 2 paths, got %d", len(paths))
	}
}

func TestWeightedShortestPath(t *testing.T) {
	db := testDB(t)

	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})
	d, _ := db.AddNode(Props{"name": "D"})

	// A -> B -> D (cost: 1 + 1 = 2)
	// A -> C -> D (cost: 5 + 1 = 6)
	// A -> D direct (cost: 10)
	db.AddEdge(a, b, "road", Props{"weight": 1.0})
	db.AddEdge(b, d, "road", Props{"weight": 1.0})
	db.AddEdge(a, c, "road", Props{"weight": 5.0})
	db.AddEdge(c, d, "road", Props{"weight": 1.0})
	db.AddEdge(a, d, "road", Props{"weight": 10.0})

	path, err := db.ShortestPathWeighted(a, d, "weight", 1.0)
	if err != nil {
		t.Fatal(err)
	}
	if path == nil {
		t.Fatal("expected a path")
	}
	if len(path.Edges) != 2 {
		t.Errorf("expected 2-hop path via B, got %d hops", len(path.Edges))
	}
}

func TestQueryBuilder(t *testing.T) {
	db := testDB(t)

	alice, _ := db.AddNode(Props{"name": "Alice", "age": 30.0})
	bob, _ := db.AddNode(Props{"name": "Bob", "age": 25.0})
	charlie, _ := db.AddNode(Props{"name": "Charlie", "age": 35.0})
	topic, _ := db.AddNode(Props{"name": "Go", "type": "topic"})

	db.AddEdge(alice, bob, "follows", nil)
	db.AddEdge(alice, charlie, "follows", nil)
	db.AddEdge(alice, topic, "consumes", nil)
	db.AddEdge(bob, charlie, "follows", nil)

	// Query: Who does Alice follow?
	result, err := db.NewQuery().
		From(alice).
		FollowEdge("follows").
		Depth(1).
		Execute()
	if err != nil {
		t.Fatal(err)
	}
	// Results include Alice herself + Bob + Charlie = 3
	// But only edges labeled "follows" are traversed from Alice at depth 1.
	if result.Count < 2 {
		t.Errorf("expected at least 2 results, got %d", result.Count)
	}

	// Query: Find followers of Alice's friends who are over 30.
	result, err = db.NewQuery().
		From(alice).
		FollowEdge("follows").
		Depth(2).
		Where(func(n *Node) bool {
			return n.GetFloat("age") > 30
		}).
		Execute()
	if err != nil {
		t.Fatal(err)
	}
	// Charlie (age 35) should be in results.
	found := false
	for _, n := range result.Nodes {
		if n.GetString("name") == "Charlie" {
			found = true
		}
	}
	if !found {
		t.Error("expected Charlie in results")
	}

	_ = bob
	_ = topic
}

func TestConcurrentQueries(t *testing.T) {
	db := testDB(t)

	// Build a graph with several nodes.
	nodes := make([]NodeID, 10)
	for i := 0; i < 10; i++ {
		id, _ := db.AddNode(Props{"name": fmt.Sprintf("Node%d", i), "index": float64(i)})
		nodes[i] = id
	}
	for i := 0; i < 9; i++ {
		db.AddEdge(nodes[i], nodes[i+1], "next", nil)
	}

	// Run concurrent queries.
	ctx := context.Background()
	cq := db.NewConcurrentQuery()

	for i := 0; i < 5; i++ {
		q := db.NewQuery().From(nodes[i]).FollowEdge("next").Depth(3)
		cq.Add(q)
	}

	results, err := cq.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}

	for i, r := range results {
		if r == nil {
			t.Errorf("result %d is nil", i)
		}
	}
}

func TestConcurrentReadWrites(t *testing.T) {
	db := testDB(t)

	// Concurrently add nodes.
	var wg sync.WaitGroup
	errors := make([]error, 100)
	ids := make([]NodeID, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id, err := db.AddNode(Props{"index": float64(idx)})
			ids[idx] = id
			errors[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Fatalf("concurrent AddNode %d failed: %v", i, err)
		}
	}

	if db.NodeCount() != 100 {
		t.Errorf("expected 100 nodes, got %d", db.NodeCount())
	}

	// Concurrently read nodes.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := db.GetNode(ids[idx])
			if err != nil {
				t.Errorf("concurrent GetNode %d failed: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestBatchOperations(t *testing.T) {
	db := testDB(t)

	// Batch add nodes.
	propsList := make([]Props, 1000)
	for i := 0; i < 1000; i++ {
		propsList[i] = Props{"name": fmt.Sprintf("Node%d", i), "index": float64(i)}
	}

	ids, err := db.AddNodeBatch(propsList)
	if err != nil {
		t.Fatalf("AddNodeBatch failed: %v", err)
	}
	if len(ids) != 1000 {
		t.Errorf("expected 1000 ids, got %d", len(ids))
	}
	if db.NodeCount() != 1000 {
		t.Errorf("expected 1000 nodes, got %d", db.NodeCount())
	}

	// Batch add edges.
	edges := make([]Edge, 999)
	for i := 0; i < 999; i++ {
		edges[i] = Edge{From: ids[i], To: ids[i+1], Label: "next"}
	}

	edgeIDs, err := db.AddEdgeBatch(edges)
	if err != nil {
		t.Fatalf("AddEdgeBatch failed: %v", err)
	}
	if len(edgeIDs) != 999 {
		t.Errorf("expected 999 edge ids, got %d", len(edgeIDs))
	}
}

func TestIndex(t *testing.T) {
	db := testDB(t)

	db.AddNode(Props{"name": "Alice", "city": "Istanbul"})
	db.AddNode(Props{"name": "Bob", "city": "Ankara"})
	db.AddNode(Props{"name": "Charlie", "city": "Istanbul"})

	// Create index on "city".
	err := db.CreateIndex("city")
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Find by indexed property.
	nodes, err := db.FindByProperty("city", "Istanbul")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes in Istanbul, got %d", len(nodes))
	}
}

func TestConnectedComponents(t *testing.T) {
	db := testDB(t)

	// Component 1: A - B - C
	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})
	db.AddEdge(a, b, "connects", nil)
	db.AddEdge(b, c, "connects", nil)

	// Component 2: D - E
	d, _ := db.AddNode(Props{"name": "D"})
	e, _ := db.AddNode(Props{"name": "E"})
	db.AddEdge(d, e, "connects", nil)

	// Component 3: F (isolated)
	db.AddNode(Props{"name": "F"})

	components, err := db.ConnectedComponents()
	if err != nil {
		t.Fatal(err)
	}
	if len(components) != 3 {
		t.Errorf("expected 3 components, got %d", len(components))
	}
}

func TestTopologicalSort(t *testing.T) {
	db := testDB(t)

	// DAG: A -> B -> D
	//      A -> C -> D
	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})
	d, _ := db.AddNode(Props{"name": "D"})

	db.AddEdge(a, b, "dep", nil)
	db.AddEdge(a, c, "dep", nil)
	db.AddEdge(b, d, "dep", nil)
	db.AddEdge(c, d, "dep", nil)

	sorted, err := db.TopologicalSort()
	if err != nil {
		t.Fatal(err)
	}
	if len(sorted) != 4 {
		t.Errorf("expected 4 nodes, got %d", len(sorted))
	}

	// A should come before B, C; B and C before D.
	pos := make(map[NodeID]int)
	for i, id := range sorted {
		pos[id] = i
	}
	if pos[a] >= pos[b] || pos[a] >= pos[c] {
		t.Error("A should come before B and C")
	}
	if pos[b] >= pos[d] || pos[c] >= pos[d] {
		t.Error("B and C should come before D")
	}
}

func TestSharding(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sharded_db")
	opts := DefaultOptions()
	opts.ShardCount = 4

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open sharded failed: %v", err)
	}
	defer db.Close()

	if !db.IsSharded() {
		t.Error("expected IsSharded to return true")
	}

	// Add nodes — they should be distributed across shards.
	for i := 0; i < 100; i++ {
		_, err := db.AddNode(Props{"index": float64(i)})
		if err != nil {
			t.Fatalf("AddNode failed: %v", err)
		}
	}

	stats, err := db.ShardStats()
	if err != nil {
		t.Fatal(err)
	}

	if len(stats) != 4 {
		t.Errorf("expected 4 shards, got %d", len(stats))
	}

	// Verify files exist on disk.
	for _, s := range stats {
		if _, err := os.Stat(s.Path); os.IsNotExist(err) {
			t.Errorf("shard file does not exist: %s", s.Path)
		}
	}

	totalNodes := uint64(0)
	for _, s := range stats {
		totalNodes += s.NodeCount
	}
	if totalNodes != 100 {
		t.Errorf("expected 100 total nodes across shards, got %d", totalNodes)
	}
}

func TestDeleteNodeCascade(t *testing.T) {
	db := testDB(t)

	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})

	db.AddEdge(a, b, "follows", nil)
	db.AddEdge(b, c, "follows", nil)
	db.AddEdge(c, a, "follows", nil) // cycle

	// Delete B — should cascade delete edges A->B and B->C.
	err := db.DeleteNode(b)
	if err != nil {
		t.Fatalf("DeleteNode failed: %v", err)
	}

	if db.NodeCount() != 2 {
		t.Errorf("expected 2 nodes after delete, got %d", db.NodeCount())
	}

	// Only C->A edge should remain.
	if db.EdgeCount() != 1 {
		t.Errorf("expected 1 edge after delete, got %d", db.EdgeCount())
	}

	// Verify A->B edge is gone.
	has, _ := db.HasEdge(a, b)
	if has {
		t.Error("edge A->B should be deleted")
	}
}

func TestNodeExistence(t *testing.T) {
	db := testDB(t)

	id, _ := db.AddNode(Props{"name": "test"})

	exists, err := db.NodeExists(id)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("node should exist")
	}

	exists, err = db.NodeExists(NodeID(99999))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("non-existent node should not exist")
	}
}

func TestDegree(t *testing.T) {
	db := testDB(t)

	a, _ := db.AddNode(Props{"name": "A"})
	b, _ := db.AddNode(Props{"name": "B"})
	c, _ := db.AddNode(Props{"name": "C"})

	db.AddEdge(a, b, "follows", nil)
	db.AddEdge(a, c, "follows", nil)
	db.AddEdge(b, a, "follows", nil)

	outDeg, err := db.Degree(a, Outgoing)
	if err != nil {
		t.Fatal(err)
	}
	if outDeg != 2 {
		t.Errorf("expected out-degree 2, got %d", outDeg)
	}

	inDeg, err := db.Degree(a, Incoming)
	if err != nil {
		t.Fatal(err)
	}
	if inDeg != 1 {
		t.Errorf("expected in-degree 1, got %d", inDeg)
	}
}

func BenchmarkAddNode(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.AddNode(Props{"index": float64(i)})
	}
}

func BenchmarkAddNodeBatch(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	batchSize := 1000
	propsList := make([]Props, batchSize)
	for i := 0; i < batchSize; i++ {
		propsList[i] = Props{"index": float64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.AddNodeBatch(propsList)
	}
}

func BenchmarkGetNode(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate.
	ids := make([]NodeID, 10000)
	propsList := make([]Props, 10000)
	for i := 0; i < 10000; i++ {
		propsList[i] = Props{"index": float64(i)}
	}
	ids, _ = db.AddNodeBatch(propsList)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.GetNode(ids[i%10000])
	}
}

func BenchmarkBFS(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Build a chain of 1000 nodes.
	propsList := make([]Props, 1000)
	for i := 0; i < 1000; i++ {
		propsList[i] = Props{"index": float64(i)}
	}
	ids, _ := db.AddNodeBatch(propsList)

	edges := make([]Edge, 999)
	for i := 0; i < 999; i++ {
		edges[i] = Edge{From: ids[i], To: ids[i+1], Label: "next"}
	}
	db.AddEdgeBatch(edges)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.BFSCollect(ids[0], 5, Outgoing)
	}
}

func BenchmarkAddEdge(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-create nodes.
	propsList := make([]Props, 1000)
	for i := 0; i < 1000; i++ {
		propsList[i] = Props{"index": float64(i)}
	}
	ids, _ := db.AddNodeBatch(propsList)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		from := ids[i%500]
		to := ids[500+i%500]
		db.AddEdge(from, to, "bench_edge", nil)
	}
}

func BenchmarkOutEdges(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Build a star graph: node 0 -> nodes 1..100.
	propsList := make([]Props, 101)
	for i := 0; i < 101; i++ {
		propsList[i] = Props{"index": float64(i)}
	}
	ids, _ := db.AddNodeBatch(propsList)

	edges := make([]Edge, 100)
	for i := 0; i < 100; i++ {
		edges[i] = Edge{From: ids[0], To: ids[i+1], Label: "knows"}
	}
	db.AddEdgeBatch(edges)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.OutEdges(ids[0])
	}
}

func BenchmarkGetNodeParallel(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	propsList := make([]Props, 10000)
	for i := 0; i < 10000; i++ {
		propsList[i] = Props{"index": float64(i)}
	}
	ids, _ := db.AddNodeBatch(propsList)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			db.GetNode(ids[i%10000])
			i++
		}
	})
}
