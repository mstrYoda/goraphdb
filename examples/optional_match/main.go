// Example: OPTIONAL MATCH
//
// Demonstrates left-outer-join semantics in Cypher:
//   - MATCH (n) OPTIONAL MATCH (n)-[r:TYPE]->(b) RETURN n, b
//   - When the optional pattern has no matches, b is nil
//   - When it does match, one row per match is emitted
//
// Run:
//
//	go run ./examples/optional_match/
package main

import (
	"fmt"
	"log"
	"os"

	graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
	fmt.Println("=== OPTIONAL MATCH Examples ===")
	fmt.Println()

	// ── Setup ────────────────────────────────────────────────────────
	dbPath := "./optional_match_example.db"
	os.RemoveAll(dbPath)

	db, err := graphdb.Open(dbPath, graphdb.DefaultOptions())
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// Build a dataset: some people wrote articles, some didn't.
	alice, _ := db.AddNodeWithLabels([]string{"Person"}, graphdb.Props{"name": "Alice"})
	bob, _ := db.AddNodeWithLabels([]string{"Person"}, graphdb.Props{"name": "Bob"})
	charlie, _ := db.AddNodeWithLabels([]string{"Person"}, graphdb.Props{"name": "Charlie"})
	diana, _ := db.AddNodeWithLabels([]string{"Person"}, graphdb.Props{"name": "Diana"})

	art1, _ := db.AddNodeWithLabels([]string{"Article"}, graphdb.Props{"title": "Graph Databases 101"})
	art2, _ := db.AddNodeWithLabels([]string{"Article"}, graphdb.Props{"title": "Go Concurrency Patterns"})
	art3, _ := db.AddNodeWithLabels([]string{"Article"}, graphdb.Props{"title": "BFS vs DFS"})

	// Alice wrote 2 articles, Bob wrote 1, Charlie and Diana wrote none.
	db.AddEdge(alice, art1, "WROTE", graphdb.Props{"year": 2024})
	db.AddEdge(alice, art2, "WROTE", graphdb.Props{"year": 2023})
	db.AddEdge(bob, art3, "WROTE", graphdb.Props{"year": 2024})

	// Alice and Bob know each other, Charlie knows Diana.
	db.AddEdge(alice, bob, "KNOWS", nil)
	db.AddEdge(bob, alice, "KNOWS", nil)
	db.AddEdge(charlie, diana, "KNOWS", nil)

	fmt.Printf("Dataset: %d nodes, %d edges\n\n", db.NodeCount(), db.EdgeCount())

	// ── 1. Basic OPTIONAL MATCH ─────────────────────────────────────
	// Find all people and what they wrote (if anything).
	// Charlie and Diana wrote nothing, so their article column will be nil.
	fmt.Println("--- MATCH (p:Person) OPTIONAL MATCH (p)-[:WROTE]->(a) RETURN p.name, a ---")
	res, err := db.Cypher(`MATCH (p:Person) OPTIONAL MATCH (p)-[:WROTE]->(a) RETURN p.name, a`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			name := row["p.name"]
			if article, ok := row["a"].(*graphdb.Node); ok && article != nil {
				fmt.Printf("    %s wrote: %s\n", name, article.GetString("title"))
			} else {
				fmt.Printf("    %s wrote: (nothing)\n", name)
			}
		}
	}

	// ── 2. OPTIONAL MATCH with relationship variable ────────────────
	// Include the edge properties (year).
	fmt.Println("\n--- MATCH (p:Person) OPTIONAL MATCH (p)-[r:WROTE]->(a) RETURN p.name, a, r ---")
	res, err = db.Cypher(`MATCH (p:Person) OPTIONAL MATCH (p)-[r:WROTE]->(a) RETURN p.name, a, r`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			name := row["p.name"]
			if article, ok := row["a"].(*graphdb.Node); ok && article != nil {
				var year any
				if edge, ok := row["r"].(*graphdb.Edge); ok && edge != nil {
					year = edge.Props["year"]
				}
				fmt.Printf("    %s wrote '%s' in %v\n", name, article.GetString("title"), year)
			} else {
				fmt.Printf("    %s wrote: (nothing) r=%v\n", name, row["r"])
			}
		}
	}

	// ── 3. Compare with regular MATCH ───────────────────────────────
	// Without OPTIONAL, only people who wrote something are returned.
	fmt.Println("\n--- Regular MATCH (no OPTIONAL) — only authors shown ---")
	res, err = db.Cypher(`MATCH (p:Person)-[:WROTE]->(a) RETURN p.name, a`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d rows (Charlie and Diana are missing!)\n", len(res.Rows))
		for _, row := range res.Rows {
			if article, ok := row["a"].(*graphdb.Node); ok {
				fmt.Printf("    %s wrote: %s\n", row["p.name"], article.GetString("title"))
			}
		}
	}

	// ── 4. OPTIONAL MATCH with different edge type ──────────────────
	// Find people and who they know (if anyone). Diana knows nobody outgoing.
	fmt.Println("\n--- MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.name, f ---")
	res, err = db.Cypher(`MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.name, f`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			name := row["p.name"]
			if friend, ok := row["f"].(*graphdb.Node); ok && friend != nil {
				fmt.Printf("    %s knows: %s\n", name, friend.GetString("name"))
			} else {
				fmt.Printf("    %s knows: (nobody)\n", name)
			}
		}
	}

	// suppress unused
	_ = charlie
	_ = diana

	fmt.Println("\n=== OPTIONAL MATCH Examples Complete ===")
}
