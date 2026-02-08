package graphdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Feature 4: Panic Recovery
// ---------------------------------------------------------------------------

func TestPanicRecovery_SafeExecute(t *testing.T) {
	err := safeExecute(func() error {
		panic("boom")
	})
	if err == nil {
		t.Fatal("expected error from panic, got nil")
	}
	if !errors.Is(err, ErrQueryPanic) {
		t.Fatalf("expected ErrQueryPanic, got: %v", err)
	}
	// Verify the error message contains the panic value and stack trace.
	if msg := err.Error(); len(msg) < 50 {
		t.Fatalf("expected detailed error with stack trace, got: %s", msg)
	}
}

func TestPanicRecovery_SafeExecuteResult(t *testing.T) {
	result, err := safeExecuteResult(func() (int, error) {
		panic("result boom")
	})
	if err == nil {
		t.Fatal("expected error from panic, got nil")
	}
	if !errors.Is(err, ErrQueryPanic) {
		t.Fatalf("expected ErrQueryPanic, got: %v", err)
	}
	if result != 0 {
		t.Fatalf("expected zero value result, got: %d", result)
	}
}

func TestPanicRecovery_NormalExecution(t *testing.T) {
	// safeExecute with no panic should work normally.
	err := safeExecute(func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	// safeExecute with a normal error should propagate the error.
	expected := fmt.Errorf("normal error")
	err = safeExecute(func() error {
		return expected
	})
	if err != expected {
		t.Fatalf("expected %v, got: %v", expected, err)
	}
}

func TestPanicRecovery_CypherEntryPoint(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A valid query should work fine.
	_, err = db.Cypher(context.Background(), `CREATE (n {name: "Alice"})`)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify the DB is still operational after normal usage.
	result, err := db.Cypher(context.Background(), `MATCH (n {name: "Alice"}) RETURN n`)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got: %d", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// Feature 2: Query Governor — MaxResultRows, DefaultQueryTimeout
// ---------------------------------------------------------------------------

func TestGovernor_MaxResultRows(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:    1,
		MaxResultRows: 5, // very low limit for testing
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert 10 nodes.
	for i := 0; i < 10; i++ {
		_, err := db.AddNode(Props{"i": i})
		if err != nil {
			t.Fatal(err)
		}
	}

	// A query returning all 10 should hit the row limit.
	_, err = db.Cypher(context.Background(), `MATCH (n) RETURN n`)
	if err == nil {
		t.Fatal("expected ErrResultTooLarge, got nil")
	}
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("expected ErrResultTooLarge, got: %v", err)
	}

	// A query with LIMIT under the cap should succeed.
	result, err := db.Cypher(context.Background(), `MATCH (n) RETURN n LIMIT 3`)
	if err != nil {
		t.Fatalf("expected no error with LIMIT, got: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got: %d", len(result.Rows))
	}
}

func TestGovernor_DefaultQueryTimeout(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:          1,
		DefaultQueryTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The governor should apply a default timeout when none is set.
	ctx := context.Background()
	wrapped, cancel := db.governor.wrapContext(ctx)
	defer cancel()

	deadline, hasDeadline := wrapped.Deadline()
	if !hasDeadline {
		t.Fatal("expected wrapped context to have a deadline")
	}
	if time.Until(deadline) > 100*time.Millisecond {
		t.Fatal("deadline is too far in the future")
	}

	// With an explicit deadline, the governor should NOT override it.
	explicitCtx, explicitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer explicitCancel()

	wrapped2, cancel2 := db.governor.wrapContext(explicitCtx)
	defer cancel2()

	deadline2, _ := wrapped2.Deadline()
	if time.Until(deadline2) < 4*time.Second {
		t.Fatal("governor should not override an explicit deadline")
	}
}

func TestGovernor_Unlimited(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:    1,
		MaxResultRows: 0, // unlimited
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert 20 nodes.
	for i := 0; i < 20; i++ {
		_, err := db.AddNode(Props{"i": i})
		if err != nil {
			t.Fatal(err)
		}
	}

	// With unlimited rows, all should be returned.
	result, err := db.Cypher(context.Background(), `MATCH (n) RETURN n`)
	if err != nil {
		t.Fatalf("expected no error with unlimited rows, got: %v", err)
	}
	if len(result.Rows) != 20 {
		t.Fatalf("expected 20 rows, got: %d", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// Feature 1: Write Backpressure
// ---------------------------------------------------------------------------

func TestBackpressure_ConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:     1,
		WriteQueueSize: 4, // small queue for testing
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Launch many concurrent writers. They should all eventually succeed
	// (the semaphore just limits concurrency, not total throughput).
	const numWriters = 50
	var wg sync.WaitGroup
	errs := make([]error, numWriters)

	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = db.AddNode(Props{"writer": idx})
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("writer %d failed: %v", i, e)
		}
	}

	// All nodes should have been written.
	count := db.NodeCount()
	if count != numWriters {
		t.Fatalf("expected %d nodes, got %d", numWriters, count)
	}
}

func TestBackpressure_WriteTimeout(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:     1,
		WriteQueueSize: 1,                    // only 1 slot
		WriteTimeout:   1 * time.Millisecond, // very short timeout
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := db.shards[0]

	// Fill the semaphore manually to simulate full queue.
	s.writeSem <- struct{}{}

	// Now try to write — should fail with ErrWriteQueueFull.
	err = s.acquireWrite(context.Background())
	if err == nil {
		t.Fatal("expected ErrWriteQueueFull, got nil")
	}
	if !errors.Is(err, ErrWriteQueueFull) {
		t.Fatalf("expected ErrWriteQueueFull, got: %v", err)
	}

	// Release the slot.
	<-s.writeSem
}

func TestBackpressure_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:     1,
		WriteQueueSize: 1, // only 1 slot
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := db.shards[0]

	// Fill the semaphore.
	s.writeSem <- struct{}{}

	// Cancel the context immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = s.acquireWrite(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
	if !errors.Is(err, ErrWriteQueueFull) {
		t.Fatalf("expected ErrWriteQueueFull, got: %v", err)
	}

	// Release the slot.
	<-s.writeSem
}

// ---------------------------------------------------------------------------
// Feature 3: Compaction / GC
// ---------------------------------------------------------------------------

func TestCompaction_Basic(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}

	// Create and then delete many nodes to generate free pages.
	var ids []NodeID
	for i := 0; i < 500; i++ {
		id, err := db.AddNode(Props{"data": fmt.Sprintf("payload-%d-padding-to-fill-pages", i)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Delete all nodes — pages go to freelist but file doesn't shrink.
	for _, id := range ids {
		if err := db.DeleteNode(id); err != nil {
			t.Fatal(err)
		}
	}

	// Record size before compaction.
	sizeBefore, _ := db.shards[0].fileSize()

	// Compact.
	saved, err := db.Compact()
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	sizeAfter, _ := db.shards[0].fileSize()

	t.Logf("before=%d after=%d saved=%d", sizeBefore, sizeAfter, saved)

	// The compacted file should be smaller (or at least not larger).
	if sizeAfter > sizeBefore {
		t.Fatalf("compacted file is larger: before=%d after=%d", sizeBefore, sizeAfter)
	}

	// DB should still be operational after compaction.
	id, err := db.AddNode(Props{"post_compact": true})
	if err != nil {
		t.Fatalf("failed to add node after compaction: %v", err)
	}

	node, err := db.GetNode(id)
	if err != nil {
		t.Fatalf("failed to get node after compaction: %v", err)
	}
	if node.Props["post_compact"] != true {
		t.Fatalf("unexpected props after compaction: %v", node.Props)
	}

	db.Close()
}

func TestCompaction_PreservesData(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}

	// Create nodes that should survive compaction.
	id1, _ := db.AddNode(Props{"name": "Alice"})
	id2, _ := db.AddNode(Props{"name": "Bob"})
	db.AddEdge(id1, id2, "KNOWS", Props{"since": "2024"})

	// Compact.
	_, err = db.Compact()
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify data is intact.
	n1, _ := db.GetNode(id1)
	if n1.Props["name"] != "Alice" {
		t.Fatalf("Alice lost after compaction")
	}
	n2, _ := db.GetNode(id2)
	if n2.Props["name"] != "Bob" {
		t.Fatalf("Bob lost after compaction")
	}

	result, err := db.Cypher(context.Background(), `MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b`)
	if err != nil {
		t.Fatalf("query failed after compaction: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 edge row, got %d", len(result.Rows))
	}

	db.Close()
}

func TestCompaction_ShardFileExists(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 2})
	if err != nil {
		t.Fatal(err)
	}

	// Insert some data.
	for i := 0; i < 10; i++ {
		db.AddNode(Props{"i": i})
	}

	// Compact all shards.
	_, err = db.Compact()
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify shard files still exist.
	for i, s := range db.shards {
		if _, statErr := os.Stat(s.path); os.IsNotExist(statErr) {
			t.Fatalf("shard %d file missing after compaction: %s", i, s.path)
		}
	}

	// Verify no temp files are left behind.
	matches, _ := filepath.Glob(filepath.Join(dir, "*.compact.tmp"))
	if len(matches) > 0 {
		t.Fatalf("temp files left behind: %v", matches)
	}

	db.Close()
}

// ---------------------------------------------------------------------------
// Feature 5: Snapshot Reads
// ---------------------------------------------------------------------------

func TestSnapshot_BasicRead(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert some data.
	db.AddNode(Props{"name": "Alice"})
	db.AddNode(Props{"name": "Bob"})

	// Take a snapshot.
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Query against the snapshot.
	result, err := snap.Cypher(context.Background(), `MATCH (n) RETURN n`)
	if err != nil {
		t.Fatalf("snapshot query failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestSnapshot_WriteRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Write queries should be rejected on snapshots.
	_, err = snap.Cypher(context.Background(), `CREATE (n {name: "Charlie"})`)
	if err == nil {
		t.Fatal("expected error for write query on snapshot, got nil")
	}
}

func TestSnapshot_ReleasedError(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Release and then try to use.
	snap.Release()

	_, err = snap.Cypher(context.Background(), `MATCH (n) RETURN n`)
	if err == nil {
		t.Fatal("expected error for released snapshot, got nil")
	}

	// Double release should be safe.
	snap.Release()
}

func TestSnapshot_DoubleRelease(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	snap.Release()
	snap.Release() // should not panic
}

func TestSnapshot_WithParams(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ShardCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.AddNode(Props{"name": "Alice", "age": 30})
	db.AddNode(Props{"name": "Bob", "age": 25})

	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	result, err := snap.CypherWithParams(context.Background(),
		`MATCH (n) WHERE n.name = $name RETURN n`,
		map[string]any{"name": "Alice"},
	)
	if err != nil {
		t.Fatalf("snapshot parameterized query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// Integration: multiple features working together
// ---------------------------------------------------------------------------

func TestIntegration_GovernorAndPanicRecovery(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:          1,
		MaxResultRows:       10,
		DefaultQueryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert 20 nodes.
	for i := 0; i < 20; i++ {
		_, err := db.AddNode(Props{"i": i})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Governor should catch the oversized result.
	_, err = db.Cypher(context.Background(), `MATCH (n) RETURN n`)
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("expected ErrResultTooLarge, got: %v", err)
	}

	// DB should remain operational after the governor rejected a query.
	result, err := db.Cypher(context.Background(), `MATCH (n) RETURN n LIMIT 5`)
	if err != nil {
		t.Fatalf("expected success with LIMIT, got: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}
}

func TestIntegration_BackpressureAndCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		ShardCount:     1,
		WriteQueueSize: 8,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent writes.
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id, err := db.AddNode(Props{"data": fmt.Sprintf("node-%d", idx)})
			if err != nil {
				return
			}
			// Delete half the nodes.
			if idx%2 == 0 {
				db.DeleteNode(id)
			}
		}(i)
	}
	wg.Wait()

	// Compact after the write storm.
	_, err = db.Compact()
	if err != nil {
		t.Fatalf("compaction after write storm failed: %v", err)
	}

	// DB should be operational.
	_, err = db.AddNode(Props{"post": true})
	if err != nil {
		t.Fatalf("post-compaction write failed: %v", err)
	}

	db.Close()
}
