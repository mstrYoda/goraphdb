package replication

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	graphdb "github.com/mstrYoda/goraphdb"
)

// TestRouter_LocalReads verifies that reads are served locally on any replica.
func TestRouter_LocalReads(t *testing.T) {
	dir := t.TempDir()
	db, _ := graphdb.Open(dir, graphdb.Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	defer db.Close()

	id, _ := db.AddNode(graphdb.Props{"name": "Alice"})

	router := NewRouter(db)

	// GetNode should work locally.
	node, err := router.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.Props["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", node.Props["name"])
	}

	// Cypher MATCH should work locally.
	result, err := router.Query(context.Background(), `MATCH (n) RETURN n`)
	if err != nil {
		t.Fatalf("Query MATCH: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestRouter_LeaderWritesLocally verifies that the leader executes writes locally.
func TestRouter_LeaderWritesLocally(t *testing.T) {
	dir := t.TempDir()
	db, _ := graphdb.Open(dir, graphdb.Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	defer db.Close()

	router := NewRouter(db)

	id, err := router.AddNode(graphdb.Props{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	node, _ := router.GetNode(id)
	if node.Props["name"] != "Bob" {
		t.Errorf("expected name=Bob, got %v", node.Props["name"])
	}
}

// TestRouter_FollowerForwardsWrites verifies that a follower forwards writes
// to the leader via HTTP.
func TestRouter_FollowerForwardsWrites(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	// Set up leader with HTTP server.
	leader, _ := graphdb.Open(leaderDir, graphdb.Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	defer leader.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/write", WriteHandler(leader))
	leaderServer := httptest.NewServer(mux)
	defer leaderServer.Close()

	// Set up follower with router pointing to leader.
	follower, _ := graphdb.Open(followerDir, graphdb.Options{
		ShardCount: 1, Role: "follower",
	})
	defer follower.Close()

	router := NewRouter(follower, WithLeaderHTTPAddr(leaderServer.URL))

	// Write through the router — should be forwarded to leader.
	id, err := router.AddNode(graphdb.Props{"name": "Charlie"})
	if err != nil {
		t.Fatalf("AddNode via router: %v", err)
	}
	if id == 0 {
		t.Fatal("expected valid node ID from leader")
	}

	// Verify the node exists on the leader.
	node, err := leader.GetNode(id)
	if err != nil {
		t.Fatalf("leader GetNode: %v", err)
	}
	if node.Props["name"] != "Charlie" {
		t.Errorf("expected name=Charlie on leader, got %v", node.Props["name"])
	}
}

// TestRouter_ForwardAddEdge verifies edge creation forwarding.
func TestRouter_ForwardAddEdge(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := graphdb.Open(leaderDir, graphdb.Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	defer leader.Close()

	// Create nodes on leader first.
	id1, _ := leader.AddNode(graphdb.Props{"name": "A"})
	id2, _ := leader.AddNode(graphdb.Props{"name": "B"})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/write", WriteHandler(leader))
	leaderServer := httptest.NewServer(mux)
	defer leaderServer.Close()

	follower, _ := graphdb.Open(followerDir, graphdb.Options{
		ShardCount: 1, Role: "follower",
	})
	defer follower.Close()

	router := NewRouter(follower, WithLeaderHTTPAddr(leaderServer.URL))

	// Forward edge creation.
	edgeID, err := router.AddEdge(id1, id2, "KNOWS", graphdb.Props{"weight": 1.0})
	if err != nil {
		t.Fatalf("AddEdge via router: %v", err)
	}

	// Verify on leader.
	edge, err := leader.GetEdge(edgeID)
	if err != nil {
		t.Fatalf("leader GetEdge: %v", err)
	}
	if edge.Label != "KNOWS" {
		t.Errorf("expected label KNOWS, got %s", edge.Label)
	}
}

// TestRouter_ForwardDeleteNode verifies delete forwarding.
func TestRouter_ForwardDeleteNode(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := graphdb.Open(leaderDir, graphdb.Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	defer leader.Close()

	id, _ := leader.AddNode(graphdb.Props{"name": "ToDelete"})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/write", WriteHandler(leader))
	leaderServer := httptest.NewServer(mux)
	defer leaderServer.Close()

	follower, _ := graphdb.Open(followerDir, graphdb.Options{
		ShardCount: 1, Role: "follower",
	})
	defer follower.Close()

	router := NewRouter(follower, WithLeaderHTTPAddr(leaderServer.URL))

	// Forward delete.
	err := router.DeleteNode(id)
	if err != nil {
		t.Fatalf("DeleteNode via router: %v", err)
	}

	// Verify node is gone on leader.
	_, err = leader.GetNode(id)
	if err == nil {
		t.Fatal("expected node to be deleted on leader")
	}
}

// TestRouter_NoLeaderError verifies the error when no leader is configured.
func TestRouter_NoLeaderError(t *testing.T) {
	dir := t.TempDir()
	db, _ := graphdb.Open(dir, graphdb.Options{
		ShardCount: 1, Role: "follower",
	})
	defer db.Close()

	router := NewRouter(db) // no leader addr

	_, err := router.AddNode(graphdb.Props{"name": "fail"})
	if err != ErrNoLeader {
		t.Fatalf("expected ErrNoLeader, got: %v", err)
	}
}

// TestRouter_DynamicLeaderAddr verifies that the leader address can be updated.
func TestRouter_DynamicLeaderAddr(t *testing.T) {
	dir := t.TempDir()
	db, _ := graphdb.Open(dir, graphdb.Options{
		ShardCount: 1, Role: "follower",
	})
	defer db.Close()

	router := NewRouter(db)

	if router.LeaderHTTPAddr() != "" {
		t.Fatal("expected empty leader addr initially")
	}

	router.SetLeaderHTTPAddr("http://leader:8080")

	if router.LeaderHTTPAddr() != "http://leader:8080" {
		t.Fatalf("expected leader addr to be set, got %s", router.LeaderHTTPAddr())
	}
}
