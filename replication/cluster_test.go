package replication

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
)

// TestCluster_AutoElectionAndReplication starts a 3-node cluster using
// StartCluster, verifies that a leader is elected automatically, writes
// data on the leader, and checks that followers receive it via replication.
func TestCluster_AutoElectionAndReplication(t *testing.T) {
	const numNodes = 3

	// Pre-allocate TCP ports for Raft and gRPC (2 ports per node).
	type nodeAddrs struct {
		raftAddr string
		grpcAddr string
	}
	allAddrs := make([]nodeAddrs, numNodes)
	for i := range allAddrs {
		raftLis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate raft port for node%d: %v", i+1, err)
		}
		grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			raftLis.Close()
			t.Fatalf("allocate grpc port for node%d: %v", i+1, err)
		}
		allAddrs[i] = nodeAddrs{
			raftAddr: raftLis.Addr().String(),
			grpcAddr: grpcLis.Addr().String(),
		}
		// Close the listeners so Raft and gRPC can bind to these ports later.
		raftLis.Close()
		grpcLis.Close()
	}

	// Open databases and start clusters.
	dbs := make([]*graphdb.DB, numNodes)
	clusters := make([]*ClusterManager, numNodes)

	for i := 0; i < numNodes; i++ {
		nodeDir := filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i+1))
		db, err := graphdb.Open(nodeDir, graphdb.Options{
			ShardCount: 1,
			EnableWAL:  true,
			WALNoSync:  true,
		})
		if err != nil {
			t.Fatalf("open db for node%d: %v", i+1, err)
		}
		dbs[i] = db

		// Build peer list (all other nodes).
		var peers []ClusterPeer
		for j := 0; j < numNodes; j++ {
			if j != i {
				peers = append(peers, ClusterPeer{
					ID:       fmt.Sprintf("node%d", j+1),
					RaftAddr: allAddrs[j].raftAddr,
					GRPCAddr: allAddrs[j].grpcAddr,
				})
			}
		}

		cm, err := StartCluster(db, ClusterConfig{
			NodeID:       fmt.Sprintf("node%d", i+1),
			RaftBindAddr: allAddrs[i].raftAddr,
			GRPCAddr:     allAddrs[i].grpcAddr,
			RaftDataDir:  filepath.Join(nodeDir, "raft"),
			Bootstrap:    true,
			Peers:        peers,
		})
		if err != nil {
			// Clean up already started clusters.
			for j := 0; j < i; j++ {
				clusters[j].Close()
				dbs[j].Close()
			}
			db.Close()
			t.Fatalf("start cluster for node%d: %v", i+1, err)
		}
		clusters[i] = cm
	}

	// Cleanup.
	defer func() {
		for i := numNodes - 1; i >= 0; i-- {
			if clusters[i] != nil {
				clusters[i].Close()
			}
			if dbs[i] != nil {
				dbs[i].Close()
			}
		}
	}()

	// ---- Phase 1: Wait for leader election ----

	leaderIdx := -1
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i, cm := range clusters {
			if cm.IsLeader() {
				leaderIdx = i
				break
			}
		}
		if leaderIdx != -1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if leaderIdx == -1 {
		t.Fatal("no leader elected within timeout")
	}

	t.Logf("leader elected: node%d", leaderIdx+1)

	// Wait a moment for role changes to propagate and replication clients to start.
	time.Sleep(1 * time.Second)

	// Verify the leader DB is in "leader" role and can accept writes.
	if role := dbs[leaderIdx].Role(); role != "leader" {
		t.Fatalf("leader DB role = %q, want 'leader'", role)
	}

	// Verify follower DBs are in "follower" role.
	for i, db := range dbs {
		if i == leaderIdx {
			continue
		}
		if role := db.Role(); role != "follower" {
			t.Fatalf("node%d DB role = %q, want 'follower'", i+1, role)
		}
	}

	// ---- Phase 2: Write data on the leader and verify replication ----

	leaderDB := dbs[leaderIdx]
	id1, err := leaderDB.AddNode(graphdb.Props{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode on leader: %v", err)
	}
	id2, err := leaderDB.AddNode(graphdb.Props{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode on leader: %v", err)
	}
	_, err = leaderDB.AddEdge(id1, id2, "KNOWS", graphdb.Props{"since": "2024"})
	if err != nil {
		t.Fatalf("AddEdge on leader: %v", err)
	}

	t.Logf("wrote 2 nodes + 1 edge on leader")

	// Wait for replication to catch up on all followers.
	lastLSN := leaderDB.WAL().LastLSN()
	for i, cm := range clusters {
		if i == leaderIdx {
			continue
		}
		followerDeadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(followerDeadline) {
			if cm.Applier().AppliedLSN() >= lastLSN {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if cm.Applier().AppliedLSN() < lastLSN {
			t.Fatalf("node%d: replication didn't catch up: applied=%d leader=%d",
				i+1, cm.Applier().AppliedLSN(), lastLSN)
		}
	}

	// Verify data exists on all followers.
	for i, db := range dbs {
		if i == leaderIdx {
			continue
		}
		node, err := db.GetNode(id1)
		if err != nil {
			t.Fatalf("node%d GetNode(%d): %v", i+1, id1, err)
		}
		if node.Props["name"] != "Alice" {
			t.Errorf("node%d: expected name=Alice, got %v", i+1, node.Props["name"])
		}

		node2, err := db.GetNode(id2)
		if err != nil {
			t.Fatalf("node%d GetNode(%d): %v", i+1, id2, err)
		}
		if node2.Props["name"] != "Bob" {
			t.Errorf("node%d: expected name=Bob, got %v", i+1, node2.Props["name"])
		}

		if db.NodeCount() != leaderDB.NodeCount() {
			t.Errorf("node%d: node count=%d, leader=%d", i+1, db.NodeCount(), leaderDB.NodeCount())
		}

		t.Logf("node%d: replication verified (nodes=%d)", i+1, db.NodeCount())
	}
}

// TestCluster_StandaloneNoElection verifies that when no cluster config is
// provided, the DB stays standalone and no election runs.
func TestCluster_StandaloneNoElection(t *testing.T) {
	dir := t.TempDir()
	db, err := graphdb.Open(dir, graphdb.Options{
		ShardCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// No cluster started — DB should default to standalone.
	if role := db.Role(); role != "" {
		t.Errorf("expected empty role for standalone, got %q", role)
	}

	// Writes should work in standalone mode.
	id, err := db.AddNode(graphdb.Props{"name": "solo"})
	if err != nil {
		t.Fatalf("AddNode in standalone: %v", err)
	}

	node, err := db.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.Props["name"] != "solo" {
		t.Errorf("expected name=solo, got %v", node.Props["name"])
	}
}

// TestCluster_RequiresWAL verifies that StartCluster fails if WAL is not enabled.
func TestCluster_RequiresWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := graphdb.Open(dir, graphdb.Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = StartCluster(db, ClusterConfig{
		NodeID:       "node1",
		RaftBindAddr: "127.0.0.1:0",
		GRPCAddr:     "127.0.0.1:0",
		RaftDataDir:  filepath.Join(dir, "raft"),
		Bootstrap:    true,
	})
	if err == nil {
		t.Fatal("expected error when WAL is not enabled")
	}
	t.Logf("got expected error: %v", err)
}

// TestParsePeers verifies the peer string parser.
func TestParsePeers(t *testing.T) {
	tests := []struct {
		input    string
		wantLen  int
		wantErr  bool
		wantID   string // first peer ID
		wantRaft string // first peer Raft addr
		wantGRPC string // first peer gRPC addr
	}{
		{input: "", wantLen: 0},
		{
			input:    "node2@10.0.1.2:7000@10.0.1.2:7001",
			wantLen:  1,
			wantID:   "node2",
			wantRaft: "10.0.1.2:7000",
			wantGRPC: "10.0.1.2:7001",
		},
		{
			input:    "node2@10.0.1.2:7000@10.0.1.2:7001,node3@10.0.1.3:7000@10.0.1.3:7001",
			wantLen:  2,
			wantID:   "node2",
			wantRaft: "10.0.1.2:7000",
			wantGRPC: "10.0.1.2:7001",
		},
		{input: "bad-format", wantErr: true},
		{input: "id@only_one_part", wantErr: true},
	}

	for _, tt := range tests {
		peers, err := ParsePeers(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParsePeers(%q): expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParsePeers(%q): %v", tt.input, err)
			continue
		}
		if len(peers) != tt.wantLen {
			t.Errorf("ParsePeers(%q): got %d peers, want %d", tt.input, len(peers), tt.wantLen)
			continue
		}
		if tt.wantLen > 0 {
			if peers[0].ID != tt.wantID {
				t.Errorf("ParsePeers(%q): first peer ID=%s, want %s", tt.input, peers[0].ID, tt.wantID)
			}
			if peers[0].RaftAddr != tt.wantRaft {
				t.Errorf("ParsePeers(%q): first peer RaftAddr=%s, want %s", tt.input, peers[0].RaftAddr, tt.wantRaft)
			}
			if peers[0].GRPCAddr != tt.wantGRPC {
				t.Errorf("ParsePeers(%q): first peer GRPCAddr=%s, want %s", tt.input, peers[0].GRPCAddr, tt.wantGRPC)
			}
		}
	}
}
