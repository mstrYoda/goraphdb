package graphdb

import (
	"context"
	"errors"
	"io"
	"testing"
)

// TestApplier_ReplayLeaderWAL simulates a leader writing data, then replays
// the WAL on a separate follower database and verifies identical state.
func TestApplier_ReplayLeaderWAL(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	// Open leader with WAL enabled.
	leader, err := Open(leaderDir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
		Role:       "leader",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Perform mutations on leader.
	id1, _ := leader.AddNode(Props{"name": "Alice", "age": 30})
	id2, _ := leader.AddNodeWithLabels([]string{"Person"}, Props{"name": "Bob"})
	leader.AddEdge(id1, id2, "KNOWS", Props{"since": "2024"})
	leader.UpdateNode(id1, Props{"city": "Istanbul"})
	leader.AddLabel(id2, "Developer")

	// Read all WAL entries from leader.
	reader, err := leader.wal.NewReader(1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	var entries []*WALEntry
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}

	// Open follower.
	follower, err := Open(followerDir, Options{
		ShardCount: 1,
		Role:       "follower",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Replay entries on follower.
	applier := NewApplier(follower)
	for _, entry := range entries {
		if err := applier.Apply(entry); err != nil {
			t.Fatalf("apply LSN %d failed: %v", entry.LSN, err)
		}
	}

	// Verify follower state matches leader.
	// Node 1: Alice with city=Istanbul
	node1, err := follower.GetNode(id1)
	if err != nil {
		t.Fatalf("follower GetNode(%d): %v", id1, err)
	}
	if node1.Props["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", node1.Props["name"])
	}
	if node1.Props["city"] != "Istanbul" {
		t.Errorf("expected city=Istanbul, got %v", node1.Props["city"])
	}

	// Node 2: Bob with labels [Person, Developer]
	node2, err := follower.GetNode(id2)
	if err != nil {
		t.Fatalf("follower GetNode(%d): %v", id2, err)
	}
	if node2.Props["name"] != "Bob" {
		t.Errorf("expected name=Bob, got %v", node2.Props["name"])
	}
	labels, _ := follower.GetLabels(id2)
	labelSet := make(map[string]bool)
	for _, l := range labels {
		labelSet[l] = true
	}
	if !labelSet["Person"] || !labelSet["Developer"] {
		t.Errorf("expected labels [Person Developer], got %v", labels)
	}

	// Edge between them.
	edges, _ := follower.OutEdges(id1)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge from id1, got %d", len(edges))
	}
	if edges[0].Label != "KNOWS" {
		t.Errorf("expected edge label KNOWS, got %s", edges[0].Label)
	}

	// Node counts should match.
	if leader.NodeCount() != follower.NodeCount() {
		t.Errorf("node count mismatch: leader=%d follower=%d",
			leader.NodeCount(), follower.NodeCount())
	}

	leader.Close()
	follower.Close()
}

// TestApplier_DeleteOperations tests that delete operations replay correctly.
func TestApplier_DeleteOperations(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := Open(leaderDir, Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})

	id1, _ := leader.AddNode(Props{"name": "X"})
	id2, _ := leader.AddNode(Props{"name": "Y"})
	edgeID, _ := leader.AddEdge(id1, id2, "LINK", nil)
	leader.DeleteEdge(edgeID)
	leader.DeleteNode(id1)

	// Collect WAL entries.
	reader, _ := leader.wal.NewReader(1)
	var entries []*WALEntry
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	reader.Close()

	// Replay on follower.
	follower, _ := Open(followerDir, Options{ShardCount: 1, Role: "follower"})
	applier := NewApplier(follower)
	for _, entry := range entries {
		if err := applier.Apply(entry); err != nil {
			t.Fatalf("apply LSN %d failed: %v", entry.LSN, err)
		}
	}

	// Node X should be gone.
	_, err := follower.GetNode(id1)
	if err == nil {
		t.Fatal("expected node X to be deleted on follower")
	}

	// Node Y should exist.
	_, err = follower.GetNode(id2)
	if err != nil {
		t.Fatalf("expected node Y to exist on follower: %v", err)
	}

	// Edge should be gone.
	_, err = follower.GetEdge(edgeID)
	if err == nil {
		t.Fatal("expected edge to be deleted on follower")
	}

	leader.Close()
	follower.Close()
}

// TestApplier_Idempotent verifies that applying the same entry twice is safe.
func TestApplier_Idempotent(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := Open(leaderDir, Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	leader.AddNode(Props{"name": "Test"})

	reader, _ := leader.wal.NewReader(1)
	entry, _ := reader.Next()
	reader.Close()

	follower, _ := Open(followerDir, Options{ShardCount: 1, Role: "follower"})
	applier := NewApplier(follower)

	// Apply once.
	if err := applier.Apply(entry); err != nil {
		t.Fatal(err)
	}

	// Apply again — should be a no-op (LSN already applied).
	if err := applier.Apply(entry); err != nil {
		t.Fatal(err)
	}

	if follower.NodeCount() != 1 {
		t.Fatalf("expected 1 node after idempotent apply, got %d", follower.NodeCount())
	}

	leader.Close()
	follower.Close()
}

// TestApplier_AppliedLSN verifies the applied LSN tracking.
func TestApplier_AppliedLSN(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := Open(leaderDir, Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true,
	})
	for i := 0; i < 5; i++ {
		leader.AddNode(Props{"i": i})
	}

	reader, _ := leader.wal.NewReader(1)
	var entries []*WALEntry
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	reader.Close()

	follower, _ := Open(followerDir, Options{ShardCount: 1, Role: "follower"})
	applier := NewApplier(follower)

	if applier.AppliedLSN() != 0 {
		t.Fatal("initial applied LSN should be 0")
	}

	for _, entry := range entries {
		applier.Apply(entry)
	}

	if applier.AppliedLSN() != entries[len(entries)-1].LSN {
		t.Fatalf("expected applied LSN %d, got %d",
			entries[len(entries)-1].LSN, applier.AppliedLSN())
	}

	leader.Close()
	follower.Close()
}

// TestFollower_RejectsWrites verifies that a follower rejects all public write API calls.
func TestFollower_RejectsWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1, Role: "follower"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// All write operations should return ErrReadOnlyReplica.
	tests := []struct {
		name string
		fn   func() error
	}{
		{"AddNode", func() error { _, err := db.AddNode(Props{"a": 1}); return err }},
		{"AddNodeBatch", func() error { _, err := db.AddNodeBatch([]Props{{"a": 1}}); return err }},
		{"UpdateNode", func() error { return db.UpdateNode(1, Props{"a": 1}) }},
		{"SetNodeProps", func() error { return db.SetNodeProps(1, Props{"a": 1}) }},
		{"DeleteNode", func() error { return db.DeleteNode(1) }},
		{"AddEdge", func() error { _, err := db.AddEdge(1, 2, "X", nil); return err }},
		{"AddEdgeBatch", func() error { _, err := db.AddEdgeBatch([]Edge{{From: 1, To: 2, Label: "X"}}); return err }},
		{"DeleteEdge", func() error { return db.DeleteEdge(1) }},
		{"UpdateEdge", func() error { return db.UpdateEdge(1, Props{"a": 1}) }},
		{"AddNodeWithLabels", func() error { _, err := db.AddNodeWithLabels([]string{"L"}, Props{"a": 1}); return err }},
		{"AddLabel", func() error { return db.AddLabel(1, "L") }},
		{"RemoveLabel", func() error { return db.RemoveLabel(1, "L") }},
		{"CreateIndex", func() error { return db.CreateIndex("a") }},
		{"DropIndex", func() error { return db.DropIndex("a") }},
		{"CreateCompositeIndex", func() error { return db.CreateCompositeIndex("a", "b") }},
		{"DropCompositeIndex", func() error { return db.DropCompositeIndex("a", "b") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if !errors.Is(err, ErrReadOnlyReplica) {
				t.Errorf("expected ErrReadOnlyReplica, got: %v", err)
			}
		})
	}
}

// TestFollower_AllowsReads verifies that a follower can serve read queries.
func TestFollower_AllowsReads(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	// Leader writes data.
	leader, _ := Open(leaderDir, Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})
	id, _ := leader.AddNode(Props{"name": "QueryMe"})

	reader, _ := leader.wal.NewReader(1)
	var entries []*WALEntry
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	reader.Close()

	// Follower replays.
	follower, _ := Open(followerDir, Options{ShardCount: 1, Role: "follower"})
	applier := NewApplier(follower)
	for _, entry := range entries {
		applier.Apply(entry)
	}

	// Follower can serve reads.
	node, err := follower.GetNode(id)
	if err != nil {
		t.Fatalf("follower read failed: %v", err)
	}
	if node.Props["name"] != "QueryMe" {
		t.Errorf("expected name=QueryMe, got %v", node.Props["name"])
	}

	// Follower can run Cypher MATCH queries.
	result, err := follower.Cypher(context.Background(), `MATCH (n) RETURN n`)
	if err != nil {
		t.Fatalf("follower Cypher MATCH failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	// Follower rejects Cypher CREATE queries.
	_, err = follower.Cypher(context.Background(), `CREATE (n {name: "Nope"})`)
	if !errors.Is(err, ErrReadOnlyReplica) {
		t.Errorf("expected ErrReadOnlyReplica for Cypher CREATE, got: %v", err)
	}

	leader.Close()
	follower.Close()
}

// TestApplier_AllOperationTypes replays all 16 operation types and verifies state.
func TestApplier_AllOperationTypes(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, _ := Open(leaderDir, Options{
		ShardCount: 1, EnableWAL: true, WALNoSync: true, Role: "leader",
	})

	// Exercise all op types on leader.
	id1, _ := leader.AddNode(Props{"name": "A"})
	id2, _ := leader.AddNodeWithLabels([]string{"Person"}, Props{"name": "B"})
	leader.AddNodeBatch([]Props{{"name": "C"}, {"name": "D"}})
	leader.UpdateNode(id1, Props{"age": 30})
	leader.SetNodeProps(id1, Props{"name": "A2", "age": 31})
	leader.AddLabel(id1, "Person")
	leader.RemoveLabel(id1, "Person")
	edgeID, _ := leader.AddEdge(id1, id2, "KNOWS", Props{})
	leader.AddEdgeBatch([]Edge{{From: id1, To: id2, Label: "LIKES"}})
	leader.UpdateEdge(edgeID, Props{"weight": 1.0})
	leader.DeleteEdge(edgeID)
	leader.CreateIndex("name")
	leader.DropIndex("name")
	leader.CreateCompositeIndex("name", "age")
	leader.DropCompositeIndex("name", "age")

	leaderNodeCount := leader.NodeCount()
	leaderEdgeCount := leader.EdgeCount()

	// Collect entries.
	reader, _ := leader.wal.NewReader(1)
	var entries []*WALEntry
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	reader.Close()

	// Replay on follower.
	follower, _ := Open(followerDir, Options{ShardCount: 1, Role: "follower"})
	applier := NewApplier(follower)
	for _, entry := range entries {
		if err := applier.Apply(entry); err != nil {
			t.Fatalf("apply LSN %d (%s) failed: %v", entry.LSN, entry.Op, err)
		}
	}

	// Verify counts match.
	if follower.NodeCount() != leaderNodeCount {
		t.Errorf("node count: leader=%d follower=%d", leaderNodeCount, follower.NodeCount())
	}
	if follower.EdgeCount() != leaderEdgeCount {
		t.Errorf("edge count: leader=%d follower=%d", leaderEdgeCount, follower.EdgeCount())
	}

	leader.Close()
	follower.Close()
}
