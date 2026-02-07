// Example: EXPLAIN and PROFILE
//
// Demonstrates:
//   - EXPLAIN — view the query plan without executing (zero I/O)
//   - PROFILE — execute and see the plan annotated with actual row counts + timing
//   - Plan operators: AllNodesScan, NodeByLabelScan, NodeIndexSeek, Filter, Expand, etc.
//
// Run:
//
//	go run ./examples/explain_profile/
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
	fmt.Println("=== EXPLAIN / PROFILE Examples ===")
	fmt.Println()

	// ── Setup ────────────────────────────────────────────────────────
	dbPath := "./explain_profile_example.db"
	os.RemoveAll(dbPath)

	db, err := graphdb.Open(dbPath, graphdb.DefaultOptions())
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// Build a dataset with labels.
	people := []struct {
		name string
		age  int
		city string
	}{
		{"Alice", 30, "Istanbul"},
		{"Bob", 25, "Ankara"},
		{"Charlie", 35, "Istanbul"},
		{"Diana", 28, "Izmir"},
		{"Eve", 32, "Istanbul"},
		{"Frank", 45, "Ankara"},
		{"Grace", 29, "Bursa"},
		{"Hank", 38, "Istanbul"},
	}

	ids := make(map[string]graphdb.NodeID)
	for _, p := range people {
		id, _ := db.AddNodeWithLabels(
			[]string{"Person"},
			graphdb.Props{"name": p.name, "age": p.age, "city": p.city},
		)
		ids[p.name] = id
	}

	// Add some movies.
	inception, _ := db.AddNodeWithLabels([]string{"Movie"}, graphdb.Props{"title": "Inception", "year": 2010})
	matrix, _ := db.AddNodeWithLabels([]string{"Movie"}, graphdb.Props{"title": "The Matrix", "year": 1999})

	// Add edges.
	db.AddEdge(ids["Alice"], ids["Bob"], "KNOWS", nil)
	db.AddEdge(ids["Alice"], ids["Charlie"], "KNOWS", nil)
	db.AddEdge(ids["Bob"], ids["Diana"], "KNOWS", nil)
	db.AddEdge(ids["Charlie"], ids["Eve"], "KNOWS", nil)
	db.AddEdge(ids["Alice"], inception, "WATCHED", graphdb.Props{"rating": 9})
	db.AddEdge(ids["Bob"], matrix, "WATCHED", graphdb.Props{"rating": 10})

	// Create index on "name".
	db.CreateIndex("name")
	db.CreateIndex("city")

	fmt.Printf("Dataset: %d nodes, %d edges\n\n", db.NodeCount(), db.EdgeCount())

	// ── 1. EXPLAIN — Full node scan ─────────────────────────────────
	fmt.Println("--- EXPLAIN: Full node scan ---")
	res, err := db.Cypher(context.Background(), `EXPLAIN MATCH (n) RETURN n`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
		fmt.Printf("  (no rows returned — EXPLAIN does not execute)\n")
		fmt.Printf("  Rows: %d\n", len(res.Rows))
	}

	// ── 2. EXPLAIN — Label scan ─────────────────────────────────────
	fmt.Println("\n--- EXPLAIN: Label scan ---")
	res, err = db.Cypher(context.Background(), `EXPLAIN MATCH (p:Person) RETURN p`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
	}

	// ── 3. EXPLAIN — Index seek with WHERE ──────────────────────────
	fmt.Println("\n--- EXPLAIN: Index seek via property filter ---")
	res, err = db.Cypher(context.Background(), `EXPLAIN MATCH (n {name: "Alice"}) RETURN n`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
	}

	// ── 4. EXPLAIN — Label scan + WHERE filter ──────────────────────
	fmt.Println("\n--- EXPLAIN: Label scan + WHERE filter ---")
	res, err = db.Cypher(context.Background(), `EXPLAIN MATCH (p:Person) WHERE p.age > 30 RETURN p`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
	}

	// ── 5. EXPLAIN — Single-hop expansion ───────────────────────────
	fmt.Println("\n--- EXPLAIN: Single-hop Expand ---")
	res, err = db.Cypher(context.Background(), `EXPLAIN MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
	}

	// ── 6. EXPLAIN — ORDER BY + LIMIT ───────────────────────────────
	fmt.Println("\n--- EXPLAIN: ORDER BY + LIMIT ---")
	res, err = db.Cypher(context.Background(), `EXPLAIN MATCH (p:Person) RETURN p ORDER BY p.age DESC LIMIT 3`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
	}

	// ── 7. PROFILE — Full node scan ─────────────────────────────────
	fmt.Println("\n--- PROFILE: Full node scan ---")
	res, err = db.Cypher(context.Background(), `PROFILE MATCH (n) RETURN n`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
		fmt.Printf("  Returned %d rows\n", len(res.Rows))
	}

	// ── 8. PROFILE — Label scan ─────────────────────────────────────
	fmt.Println("\n--- PROFILE: Label scan (Person) ---")
	res, err = db.Cypher(context.Background(), `PROFILE MATCH (p:Person) RETURN p`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
		fmt.Printf("  Returned %d Person nodes\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["p"].(*graphdb.Node); ok {
				fmt.Printf("    %s (age=%v)\n", n.GetString("name"), n.Props["age"])
			}
		}
	}

	// ── 9. PROFILE — Single-hop ─────────────────────────────────────
	fmt.Println("\n--- PROFILE: Single-hop KNOWS ---")
	res, err = db.Cypher(context.Background(), `PROFILE MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b`)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
		fmt.Printf("  Returned %d rows\n", len(res.Rows))
		for _, row := range res.Rows {
			a := row["a"].(*graphdb.Node)
			b := row["b"].(*graphdb.Node)
			fmt.Printf("    %s → %s\n", a.GetString("name"), b.GetString("name"))
		}
	}

	// ── 10. PROFILE — Parameterized with PROFILE ────────────────────
	fmt.Println("\n--- PROFILE: Parameterized query ---")
	res, err = db.CypherWithParams(context.Background(),
		`PROFILE MATCH (n {name: $name}) RETURN n`,
		map[string]any{"name": "Alice"},
	)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Print(res.Plan.String())
		fmt.Printf("  Returned %d rows\n", len(res.Rows))
	}

	// suppress unused
	_ = inception
	_ = matrix

	fmt.Println("\n=== EXPLAIN / PROFILE Examples Complete ===")
}
