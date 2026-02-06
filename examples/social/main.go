// Example: Social Network Graph
//
// Demonstrates building a small social network with people and topics,
// querying relationships, running graph traversals, and using indexes.
//
// Run:
//
//	go run ./examples/social/
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	graphdb "github.com/emre-savci/graphdb"
)

func main() {
	fmt.Println("=== Social Network Example ===")
	fmt.Println()

	// ── Open database ────────────────────────────────────────────────
	dbPath := "./social_example.db"
	os.RemoveAll(dbPath)

	opts := graphdb.DefaultOptions()
	opts.WorkerPoolSize = 8

	db, err := graphdb.Open(dbPath, opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// ── 1. Create Nodes ──────────────────────────────────────────────
	fmt.Println("--- Creating nodes ---")

	alice, _ := db.AddNode(graphdb.Props{
		"name": "Alice", "age": 30, "city": "Istanbul", "role": "engineer",
	})
	bob, _ := db.AddNode(graphdb.Props{
		"name": "Bob", "age": 25, "city": "Ankara", "role": "designer",
	})
	charlie, _ := db.AddNode(graphdb.Props{
		"name": "Charlie", "age": 35, "city": "Istanbul", "role": "manager",
	})
	diana, _ := db.AddNode(graphdb.Props{
		"name": "Diana", "age": 28, "city": "Izmir", "role": "engineer",
	})
	eve, _ := db.AddNode(graphdb.Props{
		"name": "Eve", "age": 32, "city": "Istanbul", "role": "data scientist",
	})

	// Topics / Resources
	golang, _ := db.AddNode(graphdb.Props{
		"name": "Go Programming", "type": "topic", "difficulty": "intermediate",
	})
	graphTheory, _ := db.AddNode(graphdb.Props{
		"name": "Graph Theory", "type": "topic", "difficulty": "advanced",
	})
	kubernetes, _ := db.AddNode(graphdb.Props{
		"name": "Kubernetes", "type": "topic", "difficulty": "advanced",
	})

	fmt.Printf("  Created %d nodes\n", db.NodeCount())

	// ── 2. Create Edges (Relationships) ──────────────────────────────
	fmt.Println("\n--- Creating edges ---")

	// Social: follows
	db.AddEdge(alice, bob, "follows", graphdb.Props{"since": "2023"})
	db.AddEdge(alice, charlie, "follows", graphdb.Props{"since": "2022"})
	db.AddEdge(bob, charlie, "follows", graphdb.Props{"since": "2024"})
	db.AddEdge(charlie, eve, "follows", graphdb.Props{"since": "2023"})
	db.AddEdge(diana, alice, "follows", graphdb.Props{"since": "2024"})
	db.AddEdge(eve, alice, "follows", graphdb.Props{"since": "2023"})

	// Knowledge: consumes
	db.AddEdge(alice, golang, "consumes", graphdb.Props{"progress": "80%"})
	db.AddEdge(alice, graphTheory, "consumes", graphdb.Props{"progress": "60%"})
	db.AddEdge(bob, golang, "consumes", graphdb.Props{"progress": "30%"})
	db.AddEdge(charlie, kubernetes, "consumes", graphdb.Props{"progress": "90%"})
	db.AddEdge(diana, golang, "consumes", graphdb.Props{"progress": "50%"})
	db.AddEdge(diana, graphTheory, "consumes", graphdb.Props{"progress": "40%"})
	db.AddEdge(eve, graphTheory, "consumes", graphdb.Props{"progress": "70%"})

	// Work: manages
	db.AddEdge(charlie, alice, "manages", nil)
	db.AddEdge(charlie, bob, "manages", nil)

	fmt.Printf("  Created %d edges\n", db.EdgeCount())

	// ── 3. Basic Queries ─────────────────────────────────────────────
	fmt.Println("\n--- Basic queries ---")

	node, _ := db.GetNode(alice)
	fmt.Printf("  Alice: %v\n", node.Props)

	// Who does Alice follow?
	follows, _ := db.NeighborsLabeled(alice, "follows")
	fmt.Printf("  Alice follows: ")
	for i, n := range follows {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(n.GetString("name"))
	}
	fmt.Println()

	// Who follows Alice?
	inEdges, _ := db.InEdgesLabeled(alice, "follows")
	fmt.Printf("  Alice is followed by: ")
	for i, e := range inEdges {
		if i > 0 {
			fmt.Print(", ")
		}
		fromNode, _ := db.GetNode(e.From)
		fmt.Print(fromNode.GetString("name"))
	}
	fmt.Println()

	// What does Alice consume?
	consumes, _ := db.NeighborsLabeled(alice, "consumes")
	fmt.Printf("  Alice consumes: ")
	for i, n := range consumes {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(n.GetString("name"))
	}
	fmt.Println()

	// Node degree
	outDeg, _ := db.Degree(alice, graphdb.Outgoing)
	inDeg, _ := db.Degree(alice, graphdb.Incoming)
	fmt.Printf("  Alice degree: out=%d, in=%d\n", outDeg, inDeg)

	// ── 4. BFS Traversal ─────────────────────────────────────────────
	fmt.Println("\n--- BFS from Alice (depth 2) ---")
	bfsResults, _ := db.BFSCollect(alice, 2, graphdb.Outgoing)
	for _, r := range bfsResults {
		fmt.Printf("  depth=%d: %s (ID=%d)\n", r.Depth, r.Node.GetString("name"), r.Node.ID)
	}

	// ── 5. DFS Traversal ─────────────────────────────────────────────
	fmt.Println("\n--- DFS from Alice (depth 2) ---")
	dfsResults, _ := db.DFSCollect(alice, 2, graphdb.Outgoing)
	for _, r := range dfsResults {
		fmt.Printf("  depth=%d: %s (ID=%d)\n", r.Depth, r.Node.GetString("name"), r.Node.ID)
	}

	// ── 6. Shortest Path ─────────────────────────────────────────────
	fmt.Println("\n--- Shortest path: Diana → Eve ---")
	path, _ := db.ShortestPath(diana, eve)
	if path != nil {
		fmt.Printf("  Path length: %d hops\n", len(path.Edges))
		fmt.Printf("  Route: ")
		for i, n := range path.Nodes {
			if i > 0 {
				fmt.Print(" → ")
			}
			fmt.Print(n.GetString("name"))
		}
		fmt.Println()
		for _, e := range path.Edges {
			fmt.Printf("    %s\n", e.String())
		}
	}

	// ── 7. Query Builder (Fluent API) ────────────────────────────────
	fmt.Println("\n--- Query Builder: engineers in Alice's 2-hop network ---")
	result, err := db.NewQuery().
		From(alice).
		FollowEdge("follows").
		Depth(2).
		Where(func(n *graphdb.Node) bool {
			return n.GetString("role") == "engineer" || n.GetString("role") == "data scientist"
		}).
		Execute()
	if err != nil {
		log.Printf("  Query error: %v", err)
	} else {
		for _, n := range result.Nodes {
			fmt.Printf("  %s (%s)\n", n.GetString("name"), n.GetString("role"))
		}
	}

	// ── 8. Concurrent Queries ────────────────────────────────────────
	fmt.Println("\n--- Concurrent queries ---")
	ctx := context.Background()
	start := time.Now()

	cq := db.NewConcurrentQuery()
	cq.Add(db.NewQuery().From(alice).FollowEdge("follows").Depth(2))
	cq.Add(db.NewQuery().From(bob).FollowEdge("follows").Depth(2))
	cq.Add(db.NewQuery().From(charlie).FollowEdge("follows").Depth(2))
	cq.Add(db.NewQuery().From(diana).FollowEdge("follows").Depth(2))
	cq.Add(db.NewQuery().From(eve).FollowEdge("follows").Depth(2))

	results, err := cq.Execute(ctx)
	if err != nil {
		log.Printf("  Concurrent query error: %v", err)
	} else {
		elapsed := time.Since(start)
		fmt.Printf("  Executed %d queries concurrently in %v\n", len(results), elapsed)
		names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
		for i, r := range results {
			fmt.Printf("  %s's network: %d nodes\n", names[i], r.Count)
		}
	}

	// ── 9. Connected Components ──────────────────────────────────────
	fmt.Println("\n--- Connected Components ---")
	components, _ := db.ConnectedComponents()
	fmt.Printf("  Found %d connected component(s)\n", len(components))
	for i, comp := range components {
		fmt.Printf("  Component %d: %d nodes\n", i+1, len(comp))
	}

	// ── 10. Index & Find by Property ─────────────────────────────────
	fmt.Println("\n--- Index lookup ---")
	db.CreateIndex("city")
	istanbulNodes, _ := db.FindByProperty("city", "Istanbul")
	fmt.Printf("  People in Istanbul: ")
	for i, n := range istanbulNodes {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(n.GetString("name"))
	}
	fmt.Println()

	// Verify auto-index maintenance: AddNode keeps the index up-to-date.
	newID, _ := db.AddNode(graphdb.Props{"name": "Fatma", "city": "Istanbul"})
	found, _ := db.FindByProperty("city", "Istanbul")
	fmt.Printf("  After AddNode(Fatma, Istanbul): %d Istanbul nodes (auto-indexed)\n", len(found))
	db.DeleteNode(newID)

	// ── 11. Stats ────────────────────────────────────────────────────
	fmt.Println("\n--- Database stats ---")
	stats, _ := db.Stats()
	fmt.Printf("  Nodes:     %d\n", stats.NodeCount)
	fmt.Printf("  Edges:     %d\n", stats.EdgeCount)
	fmt.Printf("  Shards:    %d\n", stats.ShardCount)
	fmt.Printf("  Disk size: %.2f MB\n", float64(stats.DiskSizeBytes)/1024/1024)

	// suppress unused warnings
	_ = golang
	_ = graphTheory
	_ = kubernetes

	fmt.Println("\n=== Social Network Example Complete ===")
}
