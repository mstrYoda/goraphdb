package graphdb

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestCreateUniqueConstraint_Basic(t *testing.T) {
	db := testDB(t)

	// Create some nodes first.
	id1, err := db.AddNodeWithLabels([]string{"Person"}, Props{"email": "alice@example.com", "name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	id2, err := db.AddNodeWithLabels([]string{"Person"}, Props{"email": "bob@example.com", "name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}

	// Create unique constraint.
	if err := db.CreateUniqueConstraint("Person", "email"); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify constraint is registered.
	if !db.HasUniqueConstraint("Person", "email") {
		t.Fatal("expected constraint to be registered")
	}

	// List constraints.
	constraints := db.ListUniqueConstraints()
	if len(constraints) != 1 {
		t.Fatalf("expected 1 constraint, got %d", len(constraints))
	}

	// Try to create a duplicate — should fail.
	_, err = db.AddNodeWithLabels([]string{"Person"}, Props{"email": "alice@example.com", "name": "Alice2"})
	if err == nil {
		t.Fatal("expected unique constraint violation")
	}
	if !errors.Is(err, ErrUniqueConstraintViolation) {
		t.Fatalf("expected ErrUniqueConstraintViolation, got: %v", err)
	}

	// Creating with a different email should work.
	_, err = db.AddNodeWithLabels([]string{"Person"}, Props{"email": "charlie@example.com", "name": "Charlie"})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// FindByUniqueConstraint should find nodes by unique key.
	node, err := db.FindByUniqueConstraint("Person", "email", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if node == nil {
		t.Fatal("expected to find node")
	}
	if node.ID != id1 {
		t.Fatalf("expected node ID %d, got %d", id1, node.ID)
	}

	_ = id2 // just verify it was created
}

func TestUniqueConstraint_DuplicateExistingNodes(t *testing.T) {
	db := testDB(t)

	// Create two nodes with the same email.
	_, _ = db.AddNodeWithLabels([]string{"Person"}, Props{"email": "dup@example.com"})
	_, _ = db.AddNodeWithLabels([]string{"Person"}, Props{"email": "dup@example.com"})

	// Creating a constraint should fail because of existing duplicates.
	err := db.CreateUniqueConstraint("Person", "email")
	if err == nil {
		t.Fatal("expected error due to existing duplicates")
	}
	if !errors.Is(err, ErrUniqueConstraintViolation) {
		t.Fatalf("expected ErrUniqueConstraintViolation, got: %v", err)
	}
}

func TestUniqueConstraint_UpdateNodeViolation(t *testing.T) {
	db := testDB(t)

	_, _ = db.AddNodeWithLabels([]string{"User"}, Props{"username": "alice"})
	id2, _ := db.AddNodeWithLabels([]string{"User"}, Props{"username": "bob"})

	if err := db.CreateUniqueConstraint("User", "username"); err != nil {
		t.Fatal(err)
	}

	// Updating bob's username to alice should fail.
	err := db.UpdateNode(id2, Props{"username": "alice"})
	if err == nil {
		t.Fatal("expected unique constraint violation")
	}
	if !errors.Is(err, ErrUniqueConstraintViolation) {
		t.Fatalf("expected ErrUniqueConstraintViolation, got: %v", err)
	}

	// Updating to a unique username should work.
	if err := db.UpdateNode(id2, Props{"username": "charlie"}); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestUniqueConstraint_DeleteNodeFreesSlot(t *testing.T) {
	db := testDB(t)

	id1, _ := db.AddNodeWithLabels([]string{"User"}, Props{"username": "alice"})

	if err := db.CreateUniqueConstraint("User", "username"); err != nil {
		t.Fatal(err)
	}

	// Delete alice.
	if err := db.DeleteNode(id1); err != nil {
		t.Fatal(err)
	}

	// Now we should be able to create another "alice".
	_, err := db.AddNodeWithLabels([]string{"User"}, Props{"username": "alice"})
	if err != nil {
		t.Fatalf("expected no error after deleting original, got: %v", err)
	}
}

func TestUniqueConstraint_DropConstraint(t *testing.T) {
	db := testDB(t)

	_, _ = db.AddNodeWithLabels([]string{"Person"}, Props{"email": "alice@example.com"})

	if err := db.CreateUniqueConstraint("Person", "email"); err != nil {
		t.Fatal(err)
	}

	// Drop the constraint.
	if err := db.DropUniqueConstraint("Person", "email"); err != nil {
		t.Fatal(err)
	}

	if db.HasUniqueConstraint("Person", "email") {
		t.Fatal("expected constraint to be dropped")
	}

	// Should now allow duplicates.
	_, err := db.AddNodeWithLabels([]string{"Person"}, Props{"email": "alice@example.com"})
	if err != nil {
		t.Fatalf("expected no error after dropping constraint, got: %v", err)
	}
}

func TestUniqueConstraint_PersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// Open and create constraint.
	db1, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db1.AddNodeWithLabels([]string{"Person"}, Props{"email": "alice@example.com"})
	if err := db1.CreateUniqueConstraint("Person", "email"); err != nil {
		t.Fatal(err)
	}
	db1.Close()

	// Reopen — constraint should be discovered.
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if !db2.HasUniqueConstraint("Person", "email") {
		t.Fatal("expected constraint to persist across restart")
	}

	// Should still enforce uniqueness.
	_, err = db2.AddNodeWithLabels([]string{"Person"}, Props{"email": "alice@example.com"})
	if err == nil {
		t.Fatal("expected unique constraint violation after restart")
	}
}

func TestCypherMerge_Basic(t *testing.T) {
	db := testDB(t)

	// Create a unique constraint for fast lookups.
	if err := db.CreateUniqueConstraint("Person", "email"); err != nil {
		t.Fatal(err)
	}

	// MERGE should create a new node since none exists.
	result1, err := db.Cypher(context.Background(), `MERGE (n:Person {email: "alice@example.com"}) RETURN n`)
	if err != nil {
		t.Fatalf("MERGE create failed: %v", err)
	}
	if len(result1.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result1.Rows))
	}

	node1, ok := result1.Rows[0]["n"].(*Node)
	if !ok {
		t.Fatal("expected *Node in result")
	}

	// MERGE again should return the SAME node (not create a new one).
	result2, err := db.Cypher(context.Background(), `MERGE (n:Person {email: "alice@example.com"}) RETURN n`)
	if err != nil {
		t.Fatalf("MERGE match failed: %v", err)
	}
	if len(result2.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result2.Rows))
	}

	node2, ok := result2.Rows[0]["n"].(*Node)
	if !ok {
		t.Fatal("expected *Node in result")
	}

	if node1.ID != node2.ID {
		t.Fatalf("expected same node (ID=%d), got different (ID=%d)", node1.ID, node2.ID)
	}

	// Total node count should be 1.
	count := db.NodeCount()
	if count != 1 {
		t.Fatalf("expected 1 node, got %d", count)
	}
}

func TestCypherMerge_WithoutConstraint(t *testing.T) {
	db := testDB(t)

	// MERGE without a unique constraint should still work (label scan fallback).
	result, err := db.Cypher(context.Background(), `MERGE (n:Person {name: "Alice"}) RETURN n`)
	if err != nil {
		t.Fatalf("MERGE failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// Second MERGE should find the existing node.
	result2, err := db.Cypher(context.Background(), `MERGE (n:Person {name: "Alice"}) RETURN n`)
	if err != nil {
		t.Fatalf("MERGE failed: %v", err)
	}

	node1 := result.Rows[0]["n"].(*Node)
	node2 := result2.Rows[0]["n"].(*Node)

	if node1.ID != node2.ID {
		t.Fatalf("expected same node, got different IDs: %d vs %d", node1.ID, node2.ID)
	}
}

// ---------------------------------------------------------------------------
// Cypher SET tests
// ---------------------------------------------------------------------------

func TestCypherSet_Basic(t *testing.T) {
	db := testDB(t)

	// Create a node.
	id, err := db.AddNode(Props{"name": "Alice", "age": int64(25)})
	if err != nil {
		t.Fatal(err)
	}

	// SET a new property and change an existing one.
	result, err := db.Cypher(context.Background(),
		`MATCH (n) WHERE id(n) = `+itoa(id)+` SET n.city = "Istanbul", n.age = 30 RETURN n`)
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// Verify changes persisted.
	node, err := db.GetNode(id)
	if err != nil {
		t.Fatal(err)
	}
	if node.Props["city"] != "Istanbul" {
		t.Errorf("expected city=Istanbul, got %v", node.Props["city"])
	}
	if node.Props["age"] != int64(30) {
		t.Errorf("expected age=30, got %v (type %T)", node.Props["age"], node.Props["age"])
	}
	if node.Props["name"] != "Alice" {
		t.Errorf("name should be unchanged, got %v", node.Props["name"])
	}
}

func TestCypherSet_FollowerRejects(t *testing.T) {
	db := testDB(t, Options{ShardCount: 1, Role: "follower"})

	_, err := db.Cypher(context.Background(),
		`MATCH (n) WHERE id(n) = 1 SET n.name = "Bob" RETURN n`)
	if !errors.Is(err, ErrReadOnlyReplica) {
		t.Fatalf("expected ErrReadOnlyReplica for SET on follower, got: %v", err)
	}
}

func TestCypherDelete_Basic(t *testing.T) {
	db := testDB(t)

	// Create a node.
	id, err := db.AddNode(Props{"name": "ToDelete"})
	if err != nil {
		t.Fatal(err)
	}

	// DELETE via Cypher.
	_, err = db.Cypher(context.Background(),
		`MATCH (n) WHERE id(n) = `+itoa(id)+` DELETE n RETURN n`)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Node should be gone.
	_, err = db.GetNode(id)
	if err == nil {
		t.Fatal("expected error after DELETE, got nil")
	}
}

// ---------------------------------------------------------------------------
// MERGE ON CREATE SET / ON MATCH SET tests
// ---------------------------------------------------------------------------

func TestCypherMerge_OnCreateSet(t *testing.T) {
	db := testDB(t)

	// MERGE creates a new node → ON CREATE SET should apply.
	result, err := db.Cypher(context.Background(),
		`MERGE (n:Person {name: "Alice"}) ON CREATE SET n.source = "created" RETURN n`)
	if err != nil {
		t.Fatalf("MERGE ON CREATE SET failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	node := result.Rows[0]["n"].(*Node)
	if node.Props["source"] != "created" {
		t.Errorf("expected source=created, got %v", node.Props["source"])
	}

	// Verify ON CREATE SET persisted.
	persisted, _ := db.GetNode(node.ID)
	if persisted.Props["source"] != "created" {
		t.Errorf("expected persisted source=created, got %v", persisted.Props["source"])
	}
}

func TestCypherMerge_OnMatchSet(t *testing.T) {
	db := testDB(t)

	// Create a node first.
	db.Cypher(context.Background(), `MERGE (n:Person {name: "Bob"}) RETURN n`)

	// MERGE again → should match → ON MATCH SET should apply.
	result, err := db.Cypher(context.Background(),
		`MERGE (n:Person {name: "Bob"}) ON MATCH SET n.updated = "yes" RETURN n`)
	if err != nil {
		t.Fatalf("MERGE ON MATCH SET failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	node := result.Rows[0]["n"].(*Node)
	if node.Props["updated"] != "yes" {
		t.Errorf("expected updated=yes, got %v", node.Props["updated"])
	}
}

func TestCypherMerge_OnCreateAndOnMatchSet(t *testing.T) {
	db := testDB(t)

	// First MERGE → creates → ON CREATE SET applies, ON MATCH SET does NOT.
	result1, err := db.Cypher(context.Background(),
		`MERGE (n:Person {name: "Eve"}) ON CREATE SET n.source = "new" ON MATCH SET n.source = "existing" RETURN n`)
	if err != nil {
		t.Fatalf("first MERGE failed: %v", err)
	}
	node1 := result1.Rows[0]["n"].(*Node)
	if node1.Props["source"] != "new" {
		t.Errorf("first MERGE: expected source=new, got %v", node1.Props["source"])
	}

	// Second MERGE → matches → ON MATCH SET applies.
	result2, err := db.Cypher(context.Background(),
		`MERGE (n:Person {name: "Eve"}) ON CREATE SET n.source = "new" ON MATCH SET n.source = "existing" RETURN n`)
	if err != nil {
		t.Fatalf("second MERGE failed: %v", err)
	}
	node2 := result2.Rows[0]["n"].(*Node)
	if node2.Props["source"] != "existing" {
		t.Errorf("second MERGE: expected source=existing, got %v", node2.Props["source"])
	}

	// Should still be just 1 node.
	if db.NodeCount() != 1 {
		t.Fatalf("expected 1 node, got %d", db.NodeCount())
	}
}

// helper to convert NodeID to string
func itoa(id NodeID) string {
	return fmt.Sprintf("%d", id)
}
