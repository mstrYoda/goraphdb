package graphdb

import (
	"context"
	"io"
	"testing"
)

func TestWAL_BasicAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add some nodes and edges.
	id1, _ := db.AddNode(Props{"name": "Alice"})
	id2, _ := db.AddNode(Props{"name": "Bob"})
	db.AddEdge(id1, id2, "KNOWS", Props{"since": "2024"})
	db.UpdateNode(id1, Props{"age": 30})
	db.DeleteNode(id2)

	// Check WAL has entries.
	lastLSN := db.wal.LastLSN()
	if lastLSN == 0 {
		t.Fatal("expected WAL entries, got 0 LSN")
	}

	// Read all entries from the beginning.
	reader, err := db.wal.NewReader(1)
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

	// We expect: AddNode(Alice), AddNode(Bob), AddEdge, UpdateNode, DeleteNode
	// DeleteNode internally also calls deleteEdgeInternal → DeleteEdge WAL entry
	// So: AddNode, AddNode, AddEdge, UpdateNode, DeleteEdge (from DeleteNode's edge cleanup), DeleteNode = 6
	if len(entries) < 5 {
		t.Fatalf("expected at least 5 WAL entries, got %d", len(entries))
	}

	// Verify first entry is AddNode.
	if entries[0].Op != OpAddNode {
		t.Fatalf("expected first entry to be AddNode, got %s", entries[0].Op)
	}
	if entries[0].LSN != 1 {
		t.Fatalf("expected first LSN to be 1, got %d", entries[0].LSN)
	}

	// Verify LSNs are monotonically increasing.
	for i := 1; i < len(entries); i++ {
		if entries[i].LSN <= entries[i-1].LSN {
			t.Fatalf("LSN not monotonically increasing at index %d: %d <= %d",
				i, entries[i].LSN, entries[i-1].LSN)
		}
	}

	db.Close()
}

func TestWAL_ReadFromLSN(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create 10 nodes.
	for i := 0; i < 10; i++ {
		db.AddNode(Props{"i": i})
	}

	// Read from LSN 5 onwards.
	reader, err := db.wal.NewReader(5)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	var count int
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if entry.LSN < 5 {
			t.Fatalf("got entry with LSN %d, expected >= 5", entry.LSN)
		}
		count++
	}

	if count == 0 {
		t.Fatal("expected entries from LSN 5, got none")
	}

	db.Close()
}

func TestWAL_PayloadDecoding(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	id, _ := db.AddNode(Props{"name": "Charlie", "age": 25})

	reader, err := db.wal.NewReader(1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	entry, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}

	if entry.Op != OpAddNode {
		t.Fatalf("expected OpAddNode, got %s", entry.Op)
	}

	// Decode the payload.
	var payload WALAddNode
	if err := decodeWALPayload(entry.Payload, &payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}

	if payload.ID != id {
		t.Fatalf("expected ID %d, got %d", id, payload.ID)
	}
	if payload.Props["name"] != "Charlie" {
		t.Fatalf("expected name=Charlie, got %v", payload.Props["name"])
	}

	db.Close()
}

func TestWAL_CrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Open, write, close.
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		db.AddNode(Props{"i": i})
	}
	lastLSN := db.wal.LastLSN()
	db.Close()

	// Reopen — WAL should recover and continue from the right LSN.
	db2, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Next LSN should be lastLSN + 1.
	if db2.wal.nextLSN.Load() != lastLSN+1 {
		t.Fatalf("expected next LSN %d, got %d", lastLSN+1, db2.wal.nextLSN.Load())
	}

	// Write more entries.
	db2.AddNode(Props{"i": 5})
	newLSN := db2.wal.LastLSN()

	if newLSN <= lastLSN {
		t.Fatalf("new LSN %d should be > old LSN %d", newLSN, lastLSN)
	}

	db2.Close()
}

func TestWAL_AllOperationTypes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Exercise all write operations.
	id1, _ := db.AddNode(Props{"name": "A"})                           // OpAddNode
	id2, _ := db.AddNodeWithLabels([]string{"Person"}, Props{"name": "B"}) // OpAddNodeWithLabels
	db.AddNodeBatch([]Props{{"name": "C"}, {"name": "D"}})             // OpAddNodeBatch
	db.UpdateNode(id1, Props{"age": 30})                                // OpUpdateNode
	db.SetNodeProps(id1, Props{"name": "A2", "age": 31})               // OpSetNodeProps
	db.AddLabel(id1, "Person")                                          // OpAddLabel
	db.RemoveLabel(id1, "Person")                                       // OpRemoveLabel
	edgeID, _ := db.AddEdge(id1, id2, "KNOWS", Props{})                // OpAddEdge
	db.UpdateEdge(edgeID, Props{"weight": 1.0})                        // OpUpdateEdge
	db.DeleteEdge(edgeID)                                               // OpDeleteEdge
	db.CreateIndex("name")                                              // OpCreateIndex
	db.DropIndex("name")                                                // OpDropIndex
	db.CreateCompositeIndex("name", "age")                              // OpCreateCompositeIndex
	db.DropCompositeIndex("name", "age")                                // OpDropCompositeIndex

	// Read all entries.
	reader, err := db.wal.NewReader(1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	opTypes := make(map[OpType]int)
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		opTypes[entry.Op]++
	}

	// Verify all operation types were recorded.
	expected := []OpType{
		OpAddNode, OpAddNodeWithLabels, OpAddNodeBatch,
		OpUpdateNode, OpSetNodeProps,
		OpAddLabel, OpRemoveLabel,
		OpAddEdge, OpUpdateEdge, OpDeleteEdge,
		OpCreateIndex, OpDropIndex,
		OpCreateCompositeIndex, OpDropCompositeIndex,
	}
	for _, op := range expected {
		if opTypes[op] == 0 {
			t.Errorf("missing WAL entry for %s", op)
		}
	}
}

func TestWAL_DisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// WAL should be nil when not enabled.
	if db.wal != nil {
		t.Fatal("WAL should be nil when EnableWAL is false")
	}

	// Writes should work without WAL.
	_, err = db.AddNode(Props{"name": "test"})
	if err != nil {
		t.Fatalf("write should work without WAL: %v", err)
	}
}

func TestWAL_EdgeBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id1, _ := db.AddNode(Props{"name": "A"})
	id2, _ := db.AddNode(Props{"name": "B"})
	id3, _ := db.AddNode(Props{"name": "C"})

	edges := []Edge{
		{From: id1, To: id2, Label: "KNOWS"},
		{From: id2, To: id3, Label: "KNOWS"},
	}
	db.AddEdgeBatch(edges)

	reader, err := db.wal.NewReader(1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	foundBatch := false
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if entry.Op == OpAddEdgeBatch {
			foundBatch = true
			var payload WALAddEdgeBatch
			if err := decodeWALPayload(entry.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload.Edges) != 2 {
				t.Fatalf("expected 2 edges in batch, got %d", len(payload.Edges))
			}
		}
	}
	if !foundBatch {
		t.Fatal("expected OpAddEdgeBatch entry")
	}
}

func TestWAL_SegmentPruning(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write some entries.
	for i := 0; i < 10; i++ {
		db.AddNode(Props{"i": i})
	}

	// Prune segments before LSN 1000 (should prune nothing since we have < 1000 entries).
	pruned, err := db.wal.PruneSegmentsBefore(1000)
	if err != nil {
		t.Fatal(err)
	}
	// Only one segment and it's the active one, so nothing should be pruned.
	if pruned != 0 {
		t.Fatalf("expected 0 pruned segments, got %d", pruned)
	}

	db.Close()
}

func TestWAL_DeleteNodeLogsEdgeDeletion(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id1, _ := db.AddNode(Props{"name": "X"})
	id2, _ := db.AddNode(Props{"name": "Y"})
	db.AddEdge(id1, id2, "LINK", nil)

	// Delete node X — should cascade-delete the edge too.
	db.DeleteNode(id1)

	reader, err := db.wal.NewReader(1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	ops := make(map[OpType]int)
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		ops[entry.Op]++
	}

	// Should have: AddNode(X), AddNode(Y), AddEdge, DeleteEdge (cascade), DeleteNode
	if ops[OpDeleteEdge] == 0 {
		t.Fatal("expected OpDeleteEdge from cascade delete, but not found")
	}
	if ops[OpDeleteNode] == 0 {
		t.Fatal("expected OpDeleteNode, but not found")
	}
}

func TestWAL_CypherCreateLogsWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount: 1,
		EnableWAL:  true,
		WALNoSync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Cypher CREATE goes through AddNode internally.
	_, err = db.Cypher(context.Background(), `CREATE (n {name: "CypherNode"})`)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := db.wal.NewReader(1)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	found := false
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if entry.Op == OpAddNode || entry.Op == OpAddNodeWithLabels {
			found = true
		}
	}
	if !found {
		t.Fatal("Cypher CREATE should produce WAL entries")
	}
}
