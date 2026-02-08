package replication

import (
	"net"
	"testing"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
	pb "github.com/mstrYoda/goraphdb/replication/proto"
	"google.golang.org/grpc"
)

// TestReplication_EndToEnd starts a leader with a gRPC replication server,
// writes data, starts a follower client, and verifies the data is replicated.
func TestReplication_EndToEnd(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	// Open leader with WAL.
	leader, err := graphdb.Open(leaderDir, graphdb.Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
		Role:       "leader",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	// Write initial data on leader.
	id1, _ := leader.AddNode(graphdb.Props{"name": "Alice"})
	id2, _ := leader.AddNodeWithLabels([]string{"Person"}, graphdb.Props{"name": "Bob"})
	leader.AddEdge(id1, id2, "KNOWS", graphdb.Props{"since": "2024"})

	// Start gRPC server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	replServer := NewServer(leader, leader.WAL(), WithPollInterval(10*time.Millisecond))
	pb.RegisterReplicationServiceServer(grpcServer, replServer)
	go grpcServer.Serve(lis)

	// Open follower.
	follower, err := graphdb.Open(followerDir, graphdb.Options{
		ShardCount: 1,
		Role:       "follower",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Start replication client.
	applier := graphdb.NewApplier(follower)
	client := NewClient(
		lis.Addr().String(),
		applier,
		WithFollowerID("test-follower"),
		WithBackoff(10*time.Millisecond, 100*time.Millisecond),
	)
	client.Start()
	// Cleanup order matters: stop client first, then gRPC server, then DBs.
	defer follower.Close()
	defer grpcServer.Stop()
	defer client.Stop()

	// Wait for replication to catch up.
	deadline := time.Now().Add(5 * time.Second)
	lastLeaderLSN := leader.WAL().LastLSN()
	for time.Now().Before(deadline) {
		if applier.AppliedLSN() >= lastLeaderLSN {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if applier.AppliedLSN() < lastLeaderLSN {
		t.Fatalf("replication didn't catch up: applied=%d leader=%d",
			applier.AppliedLSN(), lastLeaderLSN)
	}

	// Verify follower has the data.
	node1, err := follower.GetNode(id1)
	if err != nil {
		t.Fatalf("follower GetNode(%d): %v", id1, err)
	}
	if node1.Props["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", node1.Props["name"])
	}

	node2, err := follower.GetNode(id2)
	if err != nil {
		t.Fatalf("follower GetNode(%d): %v", id2, err)
	}
	if node2.Props["name"] != "Bob" {
		t.Errorf("expected name=Bob, got %v", node2.Props["name"])
	}

	edges, _ := follower.OutEdges(id1)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}

	// Write more data on leader while streaming.
	id3, _ := leader.AddNode(graphdb.Props{"name": "Charlie"})
	leader.AddEdge(id2, id3, "FOLLOWS", nil)

	// Wait for new entries to replicate.
	newLeaderLSN := leader.WAL().LastLSN()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if applier.AppliedLSN() >= newLeaderLSN {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if applier.AppliedLSN() < newLeaderLSN {
		t.Fatalf("live replication didn't catch up: applied=%d leader=%d",
			applier.AppliedLSN(), newLeaderLSN)
	}

	// Verify new data.
	node3, err := follower.GetNode(id3)
	if err != nil {
		t.Fatalf("follower GetNode(%d): %v", id3, err)
	}
	if node3.Props["name"] != "Charlie" {
		t.Errorf("expected name=Charlie, got %v", node3.Props["name"])
	}

	// Node counts should match.
	if leader.NodeCount() != follower.NodeCount() {
		t.Errorf("node count: leader=%d follower=%d", leader.NodeCount(), follower.NodeCount())
	}
}

// TestReplication_CatchUp verifies that a follower joining late catches up
// with all existing data before receiving live updates.
func TestReplication_CatchUp(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := graphdb.Open(leaderDir, graphdb.Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	defer leader.Close()

	// Write 100 nodes before the follower connects.
	for i := 0; i < 100; i++ {
		leader.AddNode(graphdb.Props{"i": i})
	}

	// Start gRPC server.
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	grpcServer := grpc.NewServer()
	pb.RegisterReplicationServiceServer(grpcServer, NewServer(leader, leader.WAL(),
		WithPollInterval(10*time.Millisecond)))
	go grpcServer.Serve(lis)

	// Now open follower and connect.
	follower, _ := graphdb.Open(followerDir, graphdb.Options{
		ShardCount: 1, Role: "follower",
	})

	applier := graphdb.NewApplier(follower)
	client := NewClient(lis.Addr().String(), applier,
		WithBackoff(10*time.Millisecond, 100*time.Millisecond))
	client.Start()
	defer follower.Close()
	defer grpcServer.Stop()
	defer client.Stop()

	// Wait for catch-up.
	lastLSN := leader.WAL().LastLSN()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if applier.AppliedLSN() >= lastLSN {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if follower.NodeCount() != 100 {
		t.Fatalf("expected 100 nodes after catch-up, got %d", follower.NodeCount())
	}
}

// TestReplication_ClientStop verifies that stopping the client is clean.
func TestReplication_ClientStop(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := graphdb.Open(leaderDir, graphdb.Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true,
	})
	defer leader.Close()

	leader.AddNode(graphdb.Props{"name": "test"})

	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	grpcServer := grpc.NewServer()
	pb.RegisterReplicationServiceServer(grpcServer, NewServer(leader, leader.WAL()))
	go grpcServer.Serve(lis)

	follower, _ := graphdb.Open(followerDir, graphdb.Options{
		ShardCount: 1, Role: "follower",
	})
	defer follower.Close()
	defer grpcServer.Stop()

	applier := graphdb.NewApplier(follower)
	client := NewClient(lis.Addr().String(), applier)
	client.Start()

	// Let it run briefly.
	time.Sleep(200 * time.Millisecond)

	// Stop should return without hanging.
	done := make(chan struct{})
	go func() {
		client.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(3 * time.Second):
		t.Fatal("client.Stop() hung for more than 3s")
	}
}
