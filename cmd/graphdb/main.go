// Command graphdb — minimal quickstart showing basic usage.
//
// For full examples see:
//
//	go run ./examples/social/      — social network: CRUD, traversals, paths
//	go run ./examples/cypher/      — Cypher query language patterns
//	go run ./examples/benchmark/   — 100K-node batch insert + performance tests
package main

import (
	"fmt"
	"log"
	"os"

	graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
	fmt.Println("=== GraphDB Quickstart ===")
	fmt.Println()

	// Open (or create) a database.
	dbPath := "./quickstart.db"
	os.RemoveAll(dbPath)
	db, err := graphdb.Open(dbPath, graphdb.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// Add nodes with arbitrary properties.
	alice, _ := db.AddNode(graphdb.Props{"name": "Alice", "age": 30})
	bob, _ := db.AddNode(graphdb.Props{"name": "Bob", "age": 25})
	charlie, _ := db.AddNode(graphdb.Props{"name": "Charlie", "age": 35})

	// Add directed labeled edges.
	db.AddEdge(alice, bob, "follows", nil)
	db.AddEdge(alice, charlie, "follows", nil)
	db.AddEdge(bob, charlie, "follows", nil)

	fmt.Printf("Nodes: %d, Edges: %d\n\n", db.NodeCount(), db.EdgeCount())

	// Native Go query: who does Alice follow?
	neighbors, _ := db.NeighborsLabeled(alice, "follows")
	fmt.Print("Alice follows: ")
	for i, n := range neighbors {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(n.GetString("name"))
	}
	fmt.Println()

	// BFS traversal from Alice (depth 2).
	fmt.Println("\nBFS from Alice (depth 2):")
	results, _ := db.BFSCollect(alice, 2, graphdb.Outgoing)
	for _, r := range results {
		fmt.Printf("  depth=%d  %s\n", r.Depth, r.Node.GetString("name"))
	}

	// Cypher query.
	fmt.Println("\nCypher: MATCH (a {name: \"Alice\"})-[:follows]->(b) RETURN b.name")
	res, err := db.Cypher(`MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range res.Rows {
		fmt.Printf("  → %v\n", row["b.name"])
	}

	// Stats.
	stats, _ := db.Stats()
	fmt.Printf("\nStats: %d nodes, %d edges, %d shard(s), %.2f KB\n",
		stats.NodeCount, stats.EdgeCount, stats.ShardCount,
		float64(stats.DiskSizeBytes)/1024)

	fmt.Println("\nFor more examples run:")
	fmt.Println("  go run ./examples/social/")
	fmt.Println("  go run ./examples/cypher/")
	fmt.Println("  go run ./examples/benchmark/")
}
