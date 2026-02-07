// Example: Cypher Query Language
//
// Demonstrates the supported subset of Cypher read queries:
//   - Full scan, property filter, WHERE clause
//   - 1-hop pattern, filtered traversal, variable-length path
//   - Any-edge type with type(), ORDER BY + LIMIT, aliases
//
// Run:
//
//	go run ./examples/cypher/
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
	fmt.Println("=== Cypher Query Examples ===")
	fmt.Println()

	// ── Setup ────────────────────────────────────────────────────────
	dbPath := "./cypher_example.db"
	os.RemoveAll(dbPath)

	db, err := graphdb.Open(dbPath, graphdb.DefaultOptions())
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// Create a small dataset.
	alice, _ := db.AddNode(graphdb.Props{"name": "Alice", "age": 30, "city": "Istanbul", "role": "engineer"})
	bob, _ := db.AddNode(graphdb.Props{"name": "Bob", "age": 25, "city": "Ankara", "role": "designer"})
	charlie, _ := db.AddNode(graphdb.Props{"name": "Charlie", "age": 35, "city": "Istanbul", "role": "manager"})
	diana, _ := db.AddNode(graphdb.Props{"name": "Diana", "age": 28, "city": "Izmir", "role": "engineer"})
	eve, _ := db.AddNode(graphdb.Props{"name": "Eve", "age": 32, "city": "Istanbul", "role": "data scientist"})

	db.AddNode(graphdb.Props{"name": "Go Programming", "type": "topic", "difficulty": "intermediate"})
	db.AddNode(graphdb.Props{"name": "Graph Theory", "type": "topic", "difficulty": "advanced"})
	db.AddNode(graphdb.Props{"name": "Kubernetes", "type": "topic", "difficulty": "advanced"})

	db.AddEdge(alice, bob, "follows", graphdb.Props{"since": "2023"})
	db.AddEdge(alice, charlie, "follows", graphdb.Props{"since": "2022"})
	db.AddEdge(bob, charlie, "follows", graphdb.Props{"since": "2024"})
	db.AddEdge(charlie, eve, "follows", graphdb.Props{"since": "2023"})
	db.AddEdge(diana, alice, "follows", graphdb.Props{"since": "2024"})
	db.AddEdge(eve, alice, "follows", graphdb.Props{"since": "2023"})

	golang, _ := db.AddNode(graphdb.Props{"name": "Go Lang", "type": "topic"})
	db.AddEdge(alice, golang, "consumes", graphdb.Props{"progress": "80%"})
	db.AddEdge(bob, golang, "consumes", graphdb.Props{"progress": "30%"})

	db.AddEdge(charlie, alice, "manages", nil)
	db.AddEdge(charlie, bob, "manages", nil)

	// Create indexes for the query optimizer.
	db.CreateIndex("name")
	db.CreateIndex("city")

	fmt.Printf("Dataset: %d nodes, %d edges\n", db.NodeCount(), db.EdgeCount())

	// ── 1. All nodes ─────────────────────────────────────────────────
	fmt.Println("\n--- MATCH (n) RETURN n ---")
	res, err := db.Cypher(context.Background(), `MATCH (n) RETURN n`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("    %s (ID=%d)\n", n.GetString("name"), n.ID)
			}
		}
	}

	// ── 2. Property filter ───────────────────────────────────────────
	fmt.Println("\n--- MATCH (n {name: \"Alice\"}) RETURN n ---")
	res, err = db.Cypher(context.Background(), `MATCH (n {name: "Alice"}) RETURN n`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("  → %v\n", n.Props)
			}
		}
	}

	// ── 3. WHERE clause ──────────────────────────────────────────────
	fmt.Println("\n--- MATCH (n) WHERE n.age > 30 RETURN n ---")
	res, err = db.Cypher(context.Background(), `MATCH (n) WHERE n.age > 30 RETURN n`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d nodes with age > 30:\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("    %s (age=%v)\n", n.GetString("name"), n.Props["age"])
			}
		}
	}

	// ── 4. 1-hop pattern ─────────────────────────────────────────────
	fmt.Println("\n--- MATCH (a)-[:follows]->(b) RETURN a, b ---")
	res, err = db.Cypher(context.Background(), `MATCH (a)-[:follows]->(b) RETURN a, b`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d follow relationships:\n", len(res.Rows))
		for _, row := range res.Rows {
			a := row["a"].(*graphdb.Node)
			b := row["b"].(*graphdb.Node)
			fmt.Printf("    %s → %s\n", a.GetString("name"), b.GetString("name"))
		}
	}

	// ── 5. Filtered traversal ────────────────────────────────────────
	fmt.Println("\n--- MATCH (a {name: \"Alice\"})-[:follows]->(b) RETURN b.name ---")
	res, err = db.Cypher(context.Background(), `MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, row := range res.Rows {
			fmt.Printf("  → Alice follows: %v\n", row["b.name"])
		}
	}

	// ── 6. Variable-length path ──────────────────────────────────────
	fmt.Println("\n--- MATCH (a {name: \"Alice\"})-[:follows*1..2]->(b) RETURN b ---")
	res, err = db.Cypher(context.Background(), `MATCH (a {name: "Alice"})-[:follows*1..2]->(b) RETURN b`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  → %d nodes reachable in 1–2 hops:\n", len(res.Rows))
		for _, row := range res.Rows {
			if b, ok := row["b"].(*graphdb.Node); ok {
				fmt.Printf("    %s\n", b.GetString("name"))
			}
		}
	}

	// ── 7. Any edge type + type() ────────────────────────────────────
	fmt.Println("\n--- MATCH (a {name: \"Alice\"})-[r]->(b) RETURN type(r), b.name ---")
	res, err = db.Cypher(context.Background(), `MATCH (a {name: "Alice"})-[r]->(b) RETURN type(r), b.name`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, row := range res.Rows {
			fmt.Printf("  → Alice -[%v]-> %v\n", row["type(r)"], row["b.name"])
		}
	}

	// ── 8. ORDER BY + LIMIT ──────────────────────────────────────────
	fmt.Println("\n--- MATCH (n) WHERE n.age > 0 RETURN n ORDER BY n.age DESC LIMIT 3 ---")
	res, err = db.Cypher(context.Background(), `MATCH (n) WHERE n.age > 0 RETURN n ORDER BY n.age DESC LIMIT 3`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("  → %s (age=%v)\n", n.GetString("name"), n.Props["age"])
			}
		}
	}

	// ── 9. Aliases in RETURN ─────────────────────────────────────────
	fmt.Println("\n--- MATCH (n {name: \"Bob\"}) RETURN n.name AS person, n.role AS job ---")
	res, err = db.Cypher(context.Background(), `MATCH (n {name: "Bob"}) RETURN n.name AS person, n.role AS job`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  Columns: %v\n", res.Columns)
		for _, row := range res.Rows {
			fmt.Printf("  → person=%v, job=%v\n", row["person"], row["job"])
		}
	}

	// ── 10. PreparedQuery (parse once, run many) ─────────────────────
	fmt.Println("\n--- PrepareCypher + ExecutePrepared ---")
	pq, err := db.PrepareCypher(`MATCH (n {name: "Alice"})-[:follows]->(b) RETURN b.name`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("  Prepared: %s\n", pq)
		res, err = db.ExecutePrepared(context.Background(), pq)
		if err != nil {
			log.Printf("Error: %v", err)
		} else {
			for _, row := range res.Rows {
				fmt.Printf("  → %v\n", row["b.name"])
			}
		}
	}

	// suppress unused warnings
	_ = diana
	_ = eve

	fmt.Println("\n=== Cypher Query Examples Complete ===")
}
