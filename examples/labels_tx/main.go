// Example: Node Labels, Transactions, and Parameterized Queries
//
// Demonstrates:
//   - First-class node labels (:Person, :Movie) with index-backed lookups
//   - Begin/Commit/Rollback transactions with read-your-writes
//   - Parameterized Cypher queries ($param substitution)
//
// Run:
//
//	go run ./examples/labels_tx/
package main

import (
	"fmt"
	"log"
	"os"

	graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
	fmt.Println("=== Labels, Transactions & Parameterized Queries ===")
	fmt.Println()

	// ── Setup ────────────────────────────────────────────────────────
	dbPath := "./labels_tx_example.db"
	os.RemoveAll(dbPath)

	db, err := graphdb.Open(dbPath, graphdb.DefaultOptions())
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// ── 1. Node Labels ──────────────────────────────────────────────
	fmt.Println("--- Node Labels ---")

	// Create nodes with labels.
	alice, _ := db.AddNodeWithLabels(
		[]string{"Person", "Employee"},
		graphdb.Props{"name": "Alice", "age": 30, "dept": "Engineering"},
	)
	bob, _ := db.AddNodeWithLabels(
		[]string{"Person", "Employee"},
		graphdb.Props{"name": "Bob", "age": 25, "dept": "Design"},
	)
	charlie, _ := db.AddNodeWithLabels(
		[]string{"Person", "Manager"},
		graphdb.Props{"name": "Charlie", "age": 40, "dept": "Engineering"},
	)
	inception, _ := db.AddNodeWithLabels(
		[]string{"Movie"},
		graphdb.Props{"title": "Inception", "year": 2010},
	)
	matrix, _ := db.AddNodeWithLabels(
		[]string{"Movie"},
		graphdb.Props{"title": "The Matrix", "year": 1999},
	)

	db.AddEdge(alice, bob, "KNOWS", nil)
	db.AddEdge(alice, charlie, "REPORTS_TO", nil)
	db.AddEdge(bob, charlie, "REPORTS_TO", nil)
	db.AddEdge(alice, inception, "WATCHED", graphdb.Props{"rating": 9})
	db.AddEdge(alice, matrix, "WATCHED", graphdb.Props{"rating": 10})
	db.AddEdge(bob, inception, "WATCHED", graphdb.Props{"rating": 8})

	// Query labels.
	labels, _ := db.GetLabels(alice)
	fmt.Printf("  Alice's labels: %v\n", labels)

	has, _ := db.HasLabel(alice, "Person")
	fmt.Printf("  Alice is a Person? %v\n", has)

	// Add a label dynamically.
	db.AddLabel(alice, "Admin")
	labels, _ = db.GetLabels(alice)
	fmt.Printf("  Alice's labels after AddLabel: %v\n", labels)

	// Remove a label.
	db.RemoveLabel(alice, "Employee")
	labels, _ = db.GetLabels(alice)
	fmt.Printf("  Alice's labels after RemoveLabel: %v\n", labels)

	// Find all Person nodes (index-backed).
	people, _ := db.FindByLabel("Person")
	fmt.Printf("  All Person nodes: %d\n", len(people))
	for _, p := range people {
		fmt.Printf("    - %s (ID=%d)\n", p.GetString("name"), p.ID)
	}

	// Find all Movie nodes.
	movies, _ := db.FindByLabel("Movie")
	fmt.Printf("  All Movie nodes: %d\n", len(movies))
	for _, m := range movies {
		fmt.Printf("    - %s (%v)\n", m.GetString("title"), m.Props["year"])
	}

	// Cypher with labels.
	fmt.Println("\n--- Cypher with Labels ---")
	res, err := db.Cypher(`MATCH (p:Person) RETURN p`)
	if err != nil {
		log.Printf("  Error: %v", err)
	} else {
		fmt.Printf("  MATCH (p:Person) → %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["p"].(*graphdb.Node); ok {
				fmt.Printf("    %s (labels=%v)\n", n.GetString("name"), n.Labels)
			}
		}
	}

	res, err = db.Cypher(`MATCH (m:Movie) RETURN m`)
	if err != nil {
		log.Printf("  Error: %v", err)
	} else {
		fmt.Printf("  MATCH (m:Movie) → %d rows\n", len(res.Rows))
	}

	// ── 2. Transactions ─────────────────────────────────────────────
	fmt.Println("\n--- Transactions ---")

	// Successful commit.
	fmt.Println("  Transaction 1: commit")
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("  Begin failed: %v", err)
	}

	diana, _ := tx.AddNode(graphdb.Props{"name": "Diana", "age": 28})
	eve, _ := tx.AddNode(graphdb.Props{"name": "Eve", "age": 35})
	tx.AddEdge(diana, eve, "KNOWS", nil)

	// Read-your-writes: visible inside the transaction before commit.
	node, err := tx.GetNode(diana)
	if err != nil {
		log.Printf("  GetNode in tx failed: %v", err)
	} else {
		fmt.Printf("    Read before commit: %s (ID=%d)\n", node.GetString("name"), node.ID)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatalf("  Commit failed: %v", err)
	}

	// Verify nodes exist after commit.
	dNode, _ := db.GetNode(diana)
	eNode, _ := db.GetNode(eve)
	fmt.Printf("    After commit: Diana=%s, Eve=%s\n", dNode.GetString("name"), eNode.GetString("name"))

	// Rollback — changes are discarded.
	fmt.Println("  Transaction 2: rollback")
	tx2, _ := db.Begin()
	ghost, _ := tx2.AddNode(graphdb.Props{"name": "Ghost"})
	fmt.Printf("    Added Ghost (ID=%d) inside tx\n", ghost)
	tx2.Rollback()

	_, err = db.GetNode(ghost)
	if err != nil {
		fmt.Printf("    After rollback: Ghost not found (expected): %v\n", err)
	}

	// ── 3. Parameterized Queries ────────────────────────────────────
	fmt.Println("\n--- Parameterized Queries ---")

	db.CreateIndex("name")

	// Simple parameter substitution.
	res, err = db.CypherWithParams(
		`MATCH (n {name: $name}) RETURN n`,
		map[string]any{"name": "Alice"},
	)
	if err != nil {
		log.Printf("  Error: %v", err)
	} else {
		fmt.Printf("  MATCH (n {name: $name}) with name='Alice' → %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("    %v\n", n.Props)
			}
		}
	}

	// WHERE clause with parameter.
	res, err = db.CypherWithParams(
		`MATCH (n) WHERE n.age > $minAge RETURN n`,
		map[string]any{"minAge": 30},
	)
	if err != nil {
		log.Printf("  Error: %v", err)
	} else {
		fmt.Printf("  MATCH (n) WHERE n.age > $minAge with minAge=30 → %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("    %s (age=%v)\n", n.GetString("name"), n.Props["age"])
			}
		}
	}

	// suppress unused
	_ = matrix
	_ = bob

	fmt.Println("\n=== Labels, Transactions & Parameterized Queries Complete ===")
}
