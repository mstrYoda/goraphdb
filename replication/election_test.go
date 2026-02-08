package replication

import (
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
		NodeID:   "node1",
		BindAddr: "127.0.0.1:0",
		DataDir:  dir,
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
