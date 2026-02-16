// Package integration contains end-to-end tests that spin up a real GoraphDB
// instance (in-memory, temporary directory) and exercise the SDK client against
// the live HTTP server.
//
// Run:
//
//	go test -v -count=1 ./...
//
// These tests require no external services — everything runs in-process.
package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
	"github.com/mstrYoda/goraphdb/server"

	sdk "github.com/mstrYoda/goraphdb/sdk/goraphdb"
)

// ---------------------------------------------------------------------------
// Test Harness
// ---------------------------------------------------------------------------

// testEnv holds a real GoraphDB instance, HTTP server, and SDK client.
type testEnv struct {
	db     *graphdb.DB
	srv    *httptest.Server
	client *sdk.Client
	tmpDir string
}

// newTestEnv starts a real GoraphDB + HTTP server and returns an SDK client.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "goraphdb-integration-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	opts := graphdb.DefaultOptions()
	opts.ShardCount = 2
	opts.WorkerPoolSize = 4
	opts.NoSync = true

	db, err := graphdb.Open(tmpDir, opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to open database: %v", err)
	}

	httpSrv := server.New(db, "")
	ts := httptest.NewServer(httpSrv)

	client := sdk.New(ts.URL, sdk.WithTimeout(10*time.Second))

	env := &testEnv{
		db:     db,
		srv:    ts,
		client: client,
		tmpDir: tmpDir,
	}

	t.Cleanup(func() {
		ts.Close()
		db.Close()
		os.RemoveAll(tmpDir)
	})

	return env
}

// ---------------------------------------------------------------------------
// Health & Stats
// ---------------------------------------------------------------------------

func TestIntegration_HealthAndStats(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Health check.
	health, err := env.client.Health(ctx)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if health.Status != "ok" {
		t.Fatalf("expected status=ok, got %s", health.Status)
	}
	if health.Role != "standalone" {
		t.Fatalf("expected role=standalone, got %s", health.Role)
	}
	if !health.Readable || !health.Writable {
		t.Fatalf("expected readable+writable, got readable=%v writable=%v",
			health.Readable, health.Writable)
	}

	// Ping (convenience).
	if err := env.client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	// Stats on empty database.
	stats, err := env.client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.NodeCount != 0 {
		t.Fatalf("expected 0 nodes, got %d", stats.NodeCount)
	}
	if stats.ShardCount != 2 {
		t.Fatalf("expected 2 shards, got %d", stats.ShardCount)
	}
}

// ---------------------------------------------------------------------------
// Full Node CRUD Lifecycle
// ---------------------------------------------------------------------------

func TestIntegration_NodeCRUD(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create node.
	id, err := env.client.CreateNode(ctx, sdk.Props{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero node ID")
	}

	// Read it back.
	node, err := env.client.GetNode(ctx, id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.ID != id {
		t.Fatalf("expected id=%d, got %d", id, node.ID)
	}
	if node.Props["name"] != "Alice" {
		t.Fatalf("expected name=Alice, got %v", node.Props["name"])
	}

	// Update it.
	err = env.client.UpdateNode(ctx, id, sdk.Props{"age": 31, "city": "Istanbul"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// Verify update.
	updated, err := env.client.GetNode(ctx, id)
	if err != nil {
		t.Fatalf("GetNode after update: %v", err)
	}
	if updated.Props["city"] != "Istanbul" {
		t.Fatalf("expected city=Istanbul, got %v", updated.Props["city"])
	}

	// Delete it.
	err = env.client.DeleteNode(ctx, id)
	if err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Verify deletion.
	_, err = env.client.GetNode(ctx, id)
	if err == nil {
		t.Fatal("expected error after deletion, got nil")
	}
	if !sdk.IsNotFound(err) {
		t.Fatalf("expected NotFound error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Edge CRUD
// ---------------------------------------------------------------------------

func TestIntegration_EdgeCRUD(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create two nodes.
	alice, err := env.client.CreateNode(ctx, sdk.Props{"name": "Alice"})
	if err != nil {
		t.Fatalf("CreateNode Alice: %v", err)
	}
	bob, err := env.client.CreateNode(ctx, sdk.Props{"name": "Bob"})
	if err != nil {
		t.Fatalf("CreateNode Bob: %v", err)
	}

	// Create edge.
	edgeID, err := env.client.CreateEdge(ctx, alice, bob, "FOLLOWS", sdk.Props{"since": "2024"})
	if err != nil {
		t.Fatalf("CreateEdge: %v", err)
	}
	if edgeID == 0 {
		t.Fatal("expected non-zero edge ID")
	}

	// Verify via stats.
	stats, err := env.client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.NodeCount != 2 {
		t.Fatalf("expected 2 nodes, got %d", stats.NodeCount)
	}
	if stats.EdgeCount != 1 {
		t.Fatalf("expected 1 edge, got %d", stats.EdgeCount)
	}

	// Delete edge.
	err = env.client.DeleteEdge(ctx, edgeID)
	if err != nil {
		t.Fatalf("DeleteEdge: %v", err)
	}

	// Verify edge deleted.
	stats, err = env.client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after delete: %v", err)
	}
	if stats.EdgeCount != 0 {
		t.Fatalf("expected 0 edges after delete, got %d", stats.EdgeCount)
	}
}

// ---------------------------------------------------------------------------
// Neighborhood
// ---------------------------------------------------------------------------

func TestIntegration_Neighborhood(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Build a small star graph: center -> n1, n2, n3.
	center, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Center"})
	n1, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Node1"})
	n2, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Node2"})
	n3, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Node3"})

	env.client.CreateEdge(ctx, center, n1, "CONNECTED", nil)
	env.client.CreateEdge(ctx, center, n2, "CONNECTED", nil)
	env.client.CreateEdge(ctx, center, n3, "CONNECTED", nil)

	// Fetch neighborhood.
	hood, err := env.client.GetNeighborhood(ctx, center)
	if err != nil {
		t.Fatalf("GetNeighborhood: %v", err)
	}

	if hood.Center.ID != center {
		t.Fatalf("expected center id=%d, got %d", center, hood.Center.ID)
	}
	if len(hood.Neighbors) != 3 {
		t.Fatalf("expected 3 neighbors, got %d", len(hood.Neighbors))
	}
	if len(hood.Edges) != 3 {
		t.Fatalf("expected 3 edges, got %d", len(hood.Edges))
	}
}

// ---------------------------------------------------------------------------
// Cypher Query
// ---------------------------------------------------------------------------

func TestIntegration_CypherQuery(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create some data.
	alice, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Alice", "role": "engineer"})
	bob, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Bob", "role": "designer"})
	env.client.CreateEdge(ctx, alice, bob, "KNOWS", sdk.Props{"since": "2023"})

	// Query all nodes.
	result, err := env.client.Query(ctx, `MATCH (n) RETURN n.name ORDER BY n.name`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.RowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount)
	}
	if result.ExecTimeMs <= 0 {
		t.Fatal("expected positive exec time")
	}

	// Verify columns.
	if len(result.Columns) != 1 || result.Columns[0] != "n.name" {
		t.Fatalf("unexpected columns: %v", result.Columns)
	}

	// Verify ordering (Alice < Bob).
	names := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		names[i] = row["n.name"].(string)
	}
	if names[0] != "Alice" || names[1] != "Bob" {
		t.Fatalf("expected [Alice, Bob], got %v", names)
	}

	// Query with relationship.
	result, err = env.client.Query(ctx,
		`MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name`)
	if err != nil {
		t.Fatalf("Query relationships: %v", err)
	}
	if result.RowCount != 1 {
		t.Fatalf("expected 1 relationship row, got %d", result.RowCount)
	}
	if result.Rows[0]["a.name"] != "Alice" || result.Rows[0]["b.name"] != "Bob" {
		t.Fatalf("unexpected row: %v", result.Rows[0])
	}
}

// ---------------------------------------------------------------------------
// Cypher CREATE / MERGE
// ---------------------------------------------------------------------------

func TestIntegration_CypherCreate(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create via Cypher.
	result, err := env.client.Query(ctx,
		`CREATE (n:Person {name: "Charlie", age: 28}) RETURN n`)
	if err != nil {
		t.Fatalf("Cypher CREATE: %v", err)
	}
	if result.RowCount != 1 {
		t.Fatalf("expected 1 row from CREATE, got %d", result.RowCount)
	}

	// Verify it exists.
	stats, err := env.client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.NodeCount != 1 {
		t.Fatalf("expected 1 node, got %d", stats.NodeCount)
	}

	// MERGE should not create a duplicate.
	_, err = env.client.Query(ctx,
		`MERGE (n:Person {name: "Charlie"}) ON MATCH SET n.updated = "yes" RETURN n`)
	if err != nil {
		t.Fatalf("Cypher MERGE: %v", err)
	}

	stats, err = env.client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after MERGE: %v", err)
	}
	if stats.NodeCount != 1 {
		t.Fatalf("expected still 1 node after MERGE, got %d", stats.NodeCount)
	}
}

// ---------------------------------------------------------------------------
// Streaming Query
// ---------------------------------------------------------------------------

func TestIntegration_StreamingQuery(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create 20 nodes.
	for i := 0; i < 20; i++ {
		_, err := env.client.CreateNode(ctx, sdk.Props{
			"name": "user_" + itoa(i),
			"seq":  float64(i),
		})
		if err != nil {
			t.Fatalf("CreateNode %d: %v", i, err)
		}
	}

	// Stream all nodes.
	iter, err := env.client.QueryStream(ctx, `MATCH (n) RETURN n.name, n.seq ORDER BY n.seq`)
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	defer iter.Close()

	// Verify columns come from X-Columns header.
	cols := iter.Columns()
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(cols), cols)
	}

	// Iterate all rows.
	var count int
	for iter.Next() {
		row := iter.Row()
		if row["n.name"] == nil {
			t.Fatalf("row %d: n.name is nil", count)
		}
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if count != 20 {
		t.Fatalf("expected 20 streamed rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Streaming Query with Collect
// ---------------------------------------------------------------------------

func TestIntegration_StreamCollect(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		env.client.CreateNode(ctx, sdk.Props{"name": "item_" + itoa(i)})
	}

	iter, err := env.client.QueryStream(ctx, `MATCH (n) RETURN n.name`)
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	defer iter.Close()

	rows, err := iter.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// Prepared Statements
// ---------------------------------------------------------------------------

func TestIntegration_PreparedStatements(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create test data.
	env.client.CreateNode(ctx, sdk.Props{"name": "Alice", "role": "engineer"})
	env.client.CreateNode(ctx, sdk.Props{"name": "Bob", "role": "designer"})
	env.client.CreateNode(ctx, sdk.Props{"name": "Charlie", "role": "engineer"})

	// Prepare a non-parameterized query (tests the prepare/execute lifecycle).
	stmt, err := env.client.Prepare(ctx, `MATCH (n) RETURN n.name, n.role ORDER BY n.name`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stmt.ID() == "" {
		t.Fatal("expected non-empty statement ID")
	}
	if stmt.Query() == "" {
		t.Fatal("expected non-empty query string")
	}

	// Execute: should return all 3 nodes.
	result, err := stmt.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", result.RowCount)
	}
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}

	// Verify ordering: Alice, Bob, Charlie.
	names := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		names[i] = row["n.name"].(string)
	}
	if names[0] != "Alice" || names[1] != "Bob" || names[2] != "Charlie" {
		t.Fatalf("expected [Alice, Bob, Charlie], got %v", names)
	}

	// Execute again (reuse prepared statement) — should give same results.
	result2, err := stmt.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("Execute reuse: %v", err)
	}
	if result2.RowCount != 3 {
		t.Fatalf("expected 3 rows on reuse, got %d", result2.RowCount)
	}

	// Prepare a parameterized query and test with params.
	stmtParam, err := env.client.Prepare(ctx, `MATCH (n {name: $name}) RETURN n.name, n.role`)
	if err != nil {
		t.Fatalf("Prepare parameterized: %v", err)
	}

	result, err = stmtParam.Execute(ctx, sdk.Props{"name": "Alice"})
	if err != nil {
		t.Fatalf("Execute parameterized: %v", err)
	}
	// Should return at least 1 result matching Alice.
	if result.RowCount < 1 {
		t.Fatalf("expected at least 1 row for Alice, got %d", result.RowCount)
	}
}

// ---------------------------------------------------------------------------
// Index Management
// ---------------------------------------------------------------------------

func TestIntegration_Indexes(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Initially no indexes.
	indexes, err := env.client.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	if len(indexes) != 0 {
		t.Fatalf("expected 0 indexes, got %d", len(indexes))
	}

	// Create some data first (needed for index to have something).
	env.client.CreateNode(ctx, sdk.Props{"name": "Alice", "city": "Istanbul"})
	env.client.CreateNode(ctx, sdk.Props{"name": "Bob", "city": "Ankara"})

	// Create index.
	err = env.client.CreateIndex(ctx, "name")
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	err = env.client.CreateIndex(ctx, "city")
	if err != nil {
		t.Fatalf("CreateIndex city: %v", err)
	}

	// List indexes.
	indexes, err = env.client.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	if len(indexes) != 2 {
		t.Fatalf("expected 2 indexes, got %d: %v", len(indexes), indexes)
	}

	// ReIndex.
	err = env.client.ReIndex(ctx, "name")
	if err != nil {
		t.Fatalf("ReIndex: %v", err)
	}

	// Drop index.
	err = env.client.DropIndex(ctx, "city")
	if err != nil {
		t.Fatalf("DropIndex: %v", err)
	}

	indexes, err = env.client.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("ListIndexes after drop: %v", err)
	}
	if len(indexes) != 1 {
		t.Fatalf("expected 1 index after drop, got %d", len(indexes))
	}
}

// ---------------------------------------------------------------------------
// Unique Constraints
// ---------------------------------------------------------------------------

func TestIntegration_Constraints(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create a constraint.
	err := env.client.CreateConstraint(ctx, "Person", "email")
	if err != nil {
		t.Fatalf("CreateConstraint: %v", err)
	}

	// List constraints.
	constraints, err := env.client.ListConstraints(ctx)
	if err != nil {
		t.Fatalf("ListConstraints: %v", err)
	}
	if len(constraints) != 1 {
		t.Fatalf("expected 1 constraint, got %d", len(constraints))
	}

	// Drop constraint.
	err = env.client.DropConstraint(ctx, "Person", "email")
	if err != nil {
		t.Fatalf("DropConstraint: %v", err)
	}

	constraints, err = env.client.ListConstraints(ctx)
	if err != nil {
		t.Fatalf("ListConstraints after drop: %v", err)
	}
	if len(constraints) != 0 {
		t.Fatalf("expected 0 constraints after drop, got %d", len(constraints))
	}
}

// ---------------------------------------------------------------------------
// Cursor Pagination
// ---------------------------------------------------------------------------

func TestIntegration_CursorPagination(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const totalNodes = 15

	// Create nodes.
	createdIDs := make([]uint64, 0, totalNodes)
	for i := 0; i < totalNodes; i++ {
		id, err := env.client.CreateNode(ctx, sdk.Props{"name": "user_" + itoa(i)})
		if err != nil {
			t.Fatalf("CreateNode %d: %v", i, err)
		}
		createdIDs = append(createdIDs, id)
	}

	// Create edges between consecutive created nodes.
	for i := 0; i+1 < len(createdIDs); i++ {
		_, err := env.client.CreateEdge(ctx, createdIDs[i], createdIDs[i+1], "NEXT", nil)
		if err != nil {
			t.Fatalf("CreateEdge %d->%d: %v", createdIDs[i], createdIDs[i+1], err)
		}
	}

	// Paginate nodes: 5 per page. Verify pagination terminates and
	// returns multiple pages.
	var allNodes []sdk.Node
	var cursor uint64
	pages := 0
	for pages < 100 { // safety limit
		p, err := env.client.ListNodesCursor(ctx, cursor, 5)
		if err != nil {
			t.Fatalf("ListNodesCursor (cursor=%d): %v", cursor, err)
		}
		allNodes = append(allNodes, p.Nodes...)
		pages++
		if !p.HasMore {
			break
		}
		cursor = p.NextCursor
	}
	if len(allNodes) == 0 {
		t.Fatal("expected some nodes from cursor pagination, got 0")
	}
	if pages < 2 {
		t.Fatalf("expected multiple pages, got %d", pages)
	}
	t.Logf("cursor pagination: %d nodes across %d pages", len(allNodes), pages)

	// Paginate edges: 5 per page. Verify pagination terminates.
	var allEdges []sdk.Edge
	var eCursor uint64
	ePages := 0
	for ePages < 100 {
		p, err := env.client.ListEdgesCursor(ctx, eCursor, 5)
		if err != nil {
			t.Fatalf("ListEdgesCursor (cursor=%d): %v", eCursor, err)
		}
		allEdges = append(allEdges, p.Edges...)
		ePages++
		if !p.HasMore {
			break
		}
		eCursor = p.NextCursor
	}
	if len(allEdges) == 0 {
		t.Fatal("expected some edges from cursor pagination, got 0")
	}
	t.Logf("edge cursor pagination: %d edges across %d pages", len(allEdges), ePages)
}

// ---------------------------------------------------------------------------
// Offset Pagination
// ---------------------------------------------------------------------------

func TestIntegration_OffsetPagination(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		env.client.CreateNode(ctx, sdk.Props{"name": "item_" + itoa(i)})
	}

	// First page.
	result, err := env.client.ListNodes(ctx, 3, 0)
	if err != nil {
		t.Fatalf("ListNodes page 1: %v", err)
	}
	if len(result.Nodes) != 3 {
		t.Fatalf("expected 3 nodes on page 1, got %d", len(result.Nodes))
	}
	if result.Total != 10 {
		t.Fatalf("expected total=10, got %d", result.Total)
	}

	// Second page.
	result, err = env.client.ListNodes(ctx, 3, 3)
	if err != nil {
		t.Fatalf("ListNodes page 2: %v", err)
	}
	if len(result.Nodes) != 3 {
		t.Fatalf("expected 3 nodes on page 2, got %d", len(result.Nodes))
	}
}

// ---------------------------------------------------------------------------
// Cluster Status (standalone mode)
// ---------------------------------------------------------------------------

func TestIntegration_ClusterStatus(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	status, err := env.client.ClusterStatus(ctx)
	if err != nil {
		t.Fatalf("ClusterStatus: %v", err)
	}
	if status.Mode != "standalone" {
		t.Fatalf("expected mode=standalone, got %s", status.Mode)
	}

	// Cluster nodes aggregator.
	nodesResult, err := env.client.ClusterNodes(ctx)
	if err != nil {
		t.Fatalf("ClusterNodes: %v", err)
	}
	if nodesResult.Mode != "standalone" {
		t.Fatalf("expected mode=standalone, got %s", nodesResult.Mode)
	}
	if len(nodesResult.Nodes) != 1 {
		t.Fatalf("expected 1 node in standalone, got %d", len(nodesResult.Nodes))
	}
}

// ---------------------------------------------------------------------------
// Cache Stats
// ---------------------------------------------------------------------------

func TestIntegration_CacheStats(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Run a query to populate cache.
	env.client.CreateNode(ctx, sdk.Props{"name": "test"})
	env.client.Query(ctx, `MATCH (n) RETURN n`)

	cs, err := env.client.CacheStats(ctx)
	if err != nil {
		t.Fatalf("CacheStats: %v", err)
	}
	if cs.QueryCache == nil {
		t.Fatal("expected non-nil query_cache")
	}
}

// ---------------------------------------------------------------------------
// Slow Queries (empty)
// ---------------------------------------------------------------------------

func TestIntegration_SlowQueries(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	queries, err := env.client.SlowQueries(ctx, 10)
	if err != nil {
		t.Fatalf("SlowQueries: %v", err)
	}
	// Should be empty on a fresh database with fast queries.
	if queries == nil {
		t.Fatal("expected non-nil slice (may be empty)")
	}
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func TestIntegration_Metrics(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	metrics, err := env.client.Metrics(ctx)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if metrics == nil {
		t.Fatal("expected non-nil metrics map")
	}
}

// ---------------------------------------------------------------------------
// Error Handling: bad Cypher
// ---------------------------------------------------------------------------

func TestIntegration_BadCypher(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	_, err := env.client.Query(ctx, `THIS IS NOT CYPHER`)
	if err == nil {
		t.Fatal("expected error for bad Cypher, got nil")
	}
	if !sdk.IsBadRequest(err) {
		t.Fatalf("expected BadRequest error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Context Cancellation
// ---------------------------------------------------------------------------

func TestIntegration_ContextCancellation(t *testing.T) {
	env := newTestEnv(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, err := env.client.Health(ctx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

// ---------------------------------------------------------------------------
// End-to-End Social Graph Scenario
// ---------------------------------------------------------------------------

func TestIntegration_SocialGraphScenario(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Build a small social network.
	alice, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Alice", "role": "engineer", "city": "Istanbul"})
	bob, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Bob", "role": "designer", "city": "Ankara"})
	charlie, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Charlie", "role": "engineer", "city": "Istanbul"})
	diana, _ := env.client.CreateNode(ctx, sdk.Props{"name": "Diana", "role": "manager", "city": "Izmir"})

	env.client.CreateEdge(ctx, alice, bob, "FOLLOWS", sdk.Props{"since": "2022"})
	env.client.CreateEdge(ctx, alice, charlie, "FOLLOWS", sdk.Props{"since": "2023"})
	env.client.CreateEdge(ctx, bob, diana, "FOLLOWS", sdk.Props{"since": "2024"})
	env.client.CreateEdge(ctx, charlie, diana, "FOLLOWS", sdk.Props{"since": "2024"})

	// Create index on city for fast lookups.
	env.client.CreateIndex(ctx, "city")

	// Query: who does Alice follow?
	result, err := env.client.Query(ctx,
		`MATCH (a {name: "Alice"})-[:FOLLOWS]->(b) RETURN b.name ORDER BY b.name`)
	if err != nil {
		t.Fatalf("Query Alice follows: %v", err)
	}
	if result.RowCount != 2 {
		t.Fatalf("expected Alice follows 2 people, got %d", result.RowCount)
	}
	if result.Rows[0]["b.name"] != "Bob" {
		t.Fatalf("expected first follow = Bob, got %v", result.Rows[0]["b.name"])
	}
	if result.Rows[1]["b.name"] != "Charlie" {
		t.Fatalf("expected second follow = Charlie, got %v", result.Rows[1]["b.name"])
	}

	// Query: who follows Diana? (2-hop from Alice).
	result, err = env.client.Query(ctx,
		`MATCH (a)-[:FOLLOWS]->(d {name: "Diana"}) RETURN a.name ORDER BY a.name`)
	if err != nil {
		t.Fatalf("Query followers of Diana: %v", err)
	}
	if result.RowCount != 2 {
		t.Fatalf("expected 2 followers of Diana, got %d", result.RowCount)
	}

	// Neighborhood of Alice: should see Bob and Charlie.
	hood, err := env.client.GetNeighborhood(ctx, alice)
	if err != nil {
		t.Fatalf("GetNeighborhood Alice: %v", err)
	}
	if len(hood.Neighbors) != 2 {
		t.Fatalf("expected 2 neighbors for Alice, got %d", len(hood.Neighbors))
	}

	// Stream all nodes and collect.
	iter, err := env.client.QueryStream(ctx, `MATCH (n) RETURN n.name ORDER BY n.name`)
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	defer iter.Close()
	rows, err := iter.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 streamed rows, got %d", len(rows))
	}

	// Verify final stats.
	stats, err := env.client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.NodeCount != 4 {
		t.Fatalf("expected 4 nodes, got %d", stats.NodeCount)
	}
	if stats.EdgeCount != 4 {
		t.Fatalf("expected 4 edges, got %d", stats.EdgeCount)
	}

	_ = diana // used in data setup
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// itoa converts an int to string (avoids importing strconv in tests).
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// Ensure the test server satisfies the http.Handler interface at compile time.
var _ http.Handler = (*server.Server)(nil)
