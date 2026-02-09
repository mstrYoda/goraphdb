package replication

import (
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
)

// TestElection_SingleNodeBootstrap verifies that a single bootstrapped node
// becomes leader and triggers the role change callback.
func TestElection_SingleNodeBootstrap(t *testing.T) {
	dir := t.TempDir()

	var isLeader atomic.Bool
	election, err := NewElection(ElectionConfig{
		NodeID:    "node1",
		BindAddr:  "127.0.0.1:0",
		DataDir:   dir,
		Bootstrap: true,
		OnRoleChange: func(leader bool) {
			isLeader.Store(leader)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer election.Close()

	// Wait for leadership.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if election.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !election.IsLeader() {
		t.Fatal("single bootstrapped node should become leader")
	}

	// Callback should have been triggered.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if isLeader.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !isLeader.Load() {
		t.Fatal("OnRoleChange callback should have been called with isLeader=true")
	}
}

// TestElection_DynamicRoleSwitch verifies that SetRole toggles write access.
func TestElection_DynamicRoleSwitch(t *testing.T) {
	dir := t.TempDir()
	db, err := graphdb.Open(dir, graphdb.Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
		Role:       "follower",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Should reject writes as follower.
	_, err = db.AddNode(graphdb.Props{"a": 1})
	if err != graphdb.ErrReadOnlyReplica {
		t.Fatalf("expected ErrReadOnlyReplica, got: %v", err)
	}

	// Switch to leader.
	db.SetRole("leader")

	// Should accept writes now.
	id, err := db.AddNode(graphdb.Props{"a": 1})
	if err != nil {
		t.Fatalf("expected write to succeed after SetRole(leader): %v", err)
	}
	if id == 0 {
		t.Fatal("expected valid node ID")
	}

	// Switch back to follower.
	db.SetRole("follower")

	// Should reject writes again.
	_, err = db.AddNode(graphdb.Props{"a": 2})
	if err != graphdb.ErrReadOnlyReplica {
		t.Fatalf("expected ErrReadOnlyReplica after SetRole(follower), got: %v", err)
	}

	// Reads should still work.
	node, err := db.GetNode(id)
	if err != nil {
		t.Fatalf("read should work on follower: %v", err)
	}
	if node.Props["a"] != int8(1) && node.Props["a"] != 1 {
		t.Errorf("expected a=1, got %v", node.Props["a"])
	}
}

// TestElection_LeaderAddrAndID verifies that leader address and ID are reported.
func TestElection_LeaderAddrAndID(t *testing.T) {
	dir := t.TempDir()

	election, err := NewElection(ElectionConfig{
		NodeID:    "node-alpha",
		BindAddr:  "127.0.0.1:0",
		DataDir:   dir,
		Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer election.Close()

	// Wait for leadership.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if election.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	id := election.LeaderID()
	if id != "node-alpha" {
		t.Errorf("expected leader ID 'node-alpha', got '%s'", id)
	}

	addr := election.LeaderAddr()
	if addr == "" {
		t.Error("expected non-empty leader address")
	}
}

// TestElection_ThreeNodeFailover bootstraps a 3-node Raft cluster, waits for
// a stable leader, kills the leader, and verifies that a new leader is elected
// from the remaining two nodes (which still form a majority).
func TestElection_ThreeNodeFailover(t *testing.T) {
	const numNodes = 3

	// Pre-allocate TCP ports so all nodes know each other's addresses
	// before the cluster starts. We grab free ports, close the listeners,
	// then hand those addresses to Raft.
	addrs := make([]string, numNodes)
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate port for node%d: %v", i+1, err)
		}
		addrs[i] = l.Addr().String()
		l.Close()
	}

	// Track role-change callbacks per node.
	var roleIsLeader [numNodes]atomic.Bool

	// Start all 3 nodes, each bootstrapping with the full cluster config.
	nodes := make([]*Election, numNodes)
	for i := 0; i < numNodes; i++ {
		var peers []PeerConfig
		for j := 0; j < numNodes; j++ {
			if j != i {
				peers = append(peers, PeerConfig{
					ID:   fmt.Sprintf("node%d", j+1),
					Addr: addrs[j],
				})
			}
		}

		idx := i // capture for closure
		e, err := NewElection(ElectionConfig{
			NodeID:    fmt.Sprintf("node%d", i+1),
			BindAddr:  addrs[i],
			DataDir:   filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i+1)),
			Bootstrap: true,
			Peers:     peers,
			OnRoleChange: func(isLeader bool) {
				roleIsLeader[idx].Store(isLeader)
			},
		})
		if err != nil {
			for _, n := range nodes {
				if n != nil {
					n.Close()
				}
			}
			t.Fatalf("start node%d: %v", i+1, err)
		}
		nodes[i] = e
	}

	// Ensure all nodes are cleaned up.
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Close()
			}
		}
	}()

	// ---- Phase 1: Wait for initial leader election ----

	leaderIdx := -1
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for i, n := range nodes {
			if n != nil && n.IsLeader() {
				count++
				leaderIdx = i
			}
		}
		if count == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if leaderIdx == -1 {
		t.Fatal("no leader elected within timeout")
	}

	// Verify exactly one leader.
	leaderCount := 0
	for _, n := range nodes {
		if n != nil && n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}

	t.Logf("initial leader: node%d (%s)", leaderIdx+1, addrs[leaderIdx])

	// All nodes should agree on who the leader is.
	expectedLeaderID := fmt.Sprintf("node%d", leaderIdx+1)
	for i, n := range nodes {
		if n == nil {
			continue
		}
		waitUntil := time.Now().Add(3 * time.Second)
		for time.Now().Before(waitUntil) {
			if n.LeaderID() == expectedLeaderID {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if id := n.LeaderID(); id != expectedLeaderID {
			t.Errorf("node%d sees leader=%s, want %s", i+1, id, expectedLeaderID)
		}
	}

	// ---- Phase 2: Kill the leader, expect re-election ----

	t.Logf("killing leader node%d...", leaderIdx+1)
	nodes[leaderIdx].Close()
	nodes[leaderIdx] = nil

	// Wait for a new leader to emerge from the surviving 2 nodes.
	newLeaderIdx := -1
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if n != nil && n.IsLeader() {
				newLeaderIdx = i
				break
			}
		}
		if newLeaderIdx != -1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if newLeaderIdx == -1 {
		t.Fatal("no new leader elected after killing original leader")
	}

	if newLeaderIdx == leaderIdx {
		t.Fatal("new leader should be a different node than the killed one")
	}

	t.Logf("new leader after failover: node%d (%s)", newLeaderIdx+1, addrs[newLeaderIdx])

	// Verify all surviving nodes agree on the new leader.
	newLeaderID := fmt.Sprintf("node%d", newLeaderIdx+1)
	for i, n := range nodes {
		if n == nil {
			continue
		}
		waitUntil := time.Now().Add(3 * time.Second)
		for time.Now().Before(waitUntil) {
			if n.LeaderID() == newLeaderID {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if id := n.LeaderID(); id != newLeaderID {
			t.Errorf("node%d sees leader=%s after failover, want %s", i+1, id, newLeaderID)
		}
	}

	// The new leader's OnRoleChange callback should have fired.
	if !roleIsLeader[newLeaderIdx].Load() {
		t.Error("expected OnRoleChange(true) on the new leader")
	}
}
