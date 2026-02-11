package graphdb

import (
	"context"
	"errors"
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
