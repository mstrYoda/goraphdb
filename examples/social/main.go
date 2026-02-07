// Example: Social Network Graph (10K users, 500 follows each)
//
// Demonstrates building a realistic social network with 10,000 users where
// each user follows 500 others (~5M edges), then runs graph traversals,
// queries, and index lookups at scale.
//
// Run:
//
//	go run ./examples/social/
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
)

const (
	numUsers       = 10_000
	followsPerUser = 500
)

func main() {
	fmt.Println("=== Social Network Example (10K users, 500 follows each) ===")
	fmt.Println()

	// ── Open database ────────────────────────────────────────────────
	dbPath := "./social_example.db"
	os.RemoveAll(dbPath)

	opts := graphdb.DefaultOptions()
	opts.WorkerPoolSize = 8
	opts.ShardCount = 4

	db, err := graphdb.Open(dbPath, opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		//os.RemoveAll(dbPath)
	}()

	rng := rand.New(rand.NewSource(42))

	firstNames := []string{
		"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
		"Ivy", "Jack", "Karen", "Leo", "Mona", "Nick", "Olivia", "Paul",
		"Quinn", "Ruby", "Steve", "Tina", "Uma", "Victor", "Wendy", "Xander",
		"Yara", "Zane", "Ayla", "Berk", "Cem", "Defne", "Emre", "Fatma",
	}
	lastNames := []string{
		"Yilmaz", "Kaya", "Demir", "Celik", "Sahin", "Ozturk", "Arslan",
		"Dogan", "Kilic", "Aydin", "Ozdemir", "Yildiz", "Polat", "Erdogan",
		"Tas", "Acar",
	}
	cities := []string{
		"Istanbul", "Ankara", "Izmir", "Bursa", "Antalya", "Adana",
		"Konya", "Trabzon", "Eskisehir", "Gaziantep",
	}
	roles := []string{
		"engineer", "designer", "manager", "data scientist", "product manager",
		"devops", "QA", "architect", "intern", "CTO",
	}

	// ── 1. Create 10K User Nodes (batch) ─────────────────────────────
	fmt.Println("--- Creating 10K user nodes (batch) ---")

	propsList := make([]graphdb.Props, numUsers)
	for i := 0; i < numUsers; i++ {
		propsList[i] = graphdb.Props{
			"name": fmt.Sprintf("%s %s",
				firstNames[rng.Intn(len(firstNames))],
				lastNames[rng.Intn(len(lastNames))]),
			"age":  float64(18 + rng.Intn(50)),
			"city": cities[rng.Intn(len(cities))],
			"role": roles[rng.Intn(len(roles))],
		}
	}

	start := time.Now()
	userIDs, err := db.AddNodeBatch(propsList)
	if err != nil {
		log.Fatalf("Batch add users failed: %v", err)
	}
	fmt.Printf("  Inserted %d user nodes in %v (%.0f nodes/sec)\n",
		numUsers, time.Since(start),
		float64(numUsers)/time.Since(start).Seconds())

	// ── 2. Create 5M Follow Edges (chunked batches) ─────────────────
	totalFollows := numUsers * followsPerUser
	fmt.Printf("\n--- Creating %dM follow edges (%d per user, chunked batches) ---\n",
		totalFollows/1_000_000, followsPerUser)

	const edgeBatchSize = 50_000 // flush every 50K edges to avoid huge txns
	edgeBatch := make([]graphdb.Edge, 0, edgeBatchSize)
	var totalInserted int

	start = time.Now()
	for i := 0; i < numUsers; i++ {
		from := userIDs[i]
		// Pick 500 unique random targets for this user.
		targets := make(map[int]struct{}, followsPerUser)
		for len(targets) < followsPerUser {
			t := rng.Intn(numUsers)
			if t == i {
				continue
			}
			if _, dup := targets[t]; dup {
				continue
			}
			targets[t] = struct{}{}
			edgeBatch = append(edgeBatch, graphdb.Edge{
				From:  from,
				To:    userIDs[t],
				Label: "follows",
				Props: graphdb.Props{"since": fmt.Sprintf("%d", 2020+rng.Intn(6))},
			})

			// Flush when chunk is full.
			if len(edgeBatch) >= edgeBatchSize {
				if _, err := db.AddEdgeBatch(edgeBatch); err != nil {
					log.Fatalf("Batch edge add failed: %v", err)
				}
				totalInserted += len(edgeBatch)
				fmt.Printf("\r  Progress: %d / %d edges (%.0f%%)   ",
					totalInserted, totalFollows,
					float64(totalInserted)/float64(totalFollows)*100)
				edgeBatch = edgeBatch[:0]
			}
		}
	}
	// Flush remaining edges.
	if len(edgeBatch) > 0 {
		if _, err := db.AddEdgeBatch(edgeBatch); err != nil {
			log.Fatalf("Batch edge add failed: %v", err)
		}
		totalInserted += len(edgeBatch)
	}
	edgeDur := time.Since(start)
	fmt.Printf("\r  Inserted %d edges in %v (%.0f edges/sec)          \n",
		totalInserted, edgeDur,
		float64(totalInserted)/edgeDur.Seconds())

	fmt.Printf("\n  Total: %d nodes, %d edges\n", db.NodeCount(), db.EdgeCount())

	// ── 3. Build Indexes ─────────────────────────────────────────────
	fmt.Println("\n--- Building indexes ---")
	start = time.Now()
	for _, prop := range []string{"name", "age", "city", "role"} {
		db.CreateIndex(prop)
	}
	fmt.Printf("  Indexes built in %v  (name, age, city, role)\n", time.Since(start))

	// ── 4. Basic Queries ─────────────────────────────────────────────
	fmt.Println("\n--- Basic queries ---")

	// Pick a sample user
	sampleIdx := 0
	sampleID := userIDs[sampleIdx]
	sampleNode, _ := db.GetNode(sampleID)
	sampleName := sampleNode.GetString("name")
	fmt.Printf("  Sample user: %s (ID=%d, age=%v, city=%s, role=%s)\n",
		sampleName, sampleID,
		sampleNode.Props["age"],
		sampleNode.GetString("city"),
		sampleNode.GetString("role"))

	// Who does this user follow?
	follows, _ := db.NeighborsLabeled(sampleID, "follows")
	fmt.Printf("  %s follows %d users\n", sampleName, len(follows))
	fmt.Printf("  First 5: ")
	for i := 0; i < 5 && i < len(follows); i++ {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(follows[i].GetString("name"))
	}
	fmt.Println()

	// Who follows this user?
	inEdges, _ := db.InEdgesLabeled(sampleID, "follows")
	fmt.Printf("  %s is followed by %d users\n", sampleName, len(inEdges))

	// Node degree
	outDeg, _ := db.Degree(sampleID, graphdb.Outgoing)
	inDeg, _ := db.Degree(sampleID, graphdb.Incoming)
	fmt.Printf("  %s degree: out=%d, in=%d\n", sampleName, outDeg, inDeg)

	// ── 5. BFS Traversal ─────────────────────────────────────────────
	fmt.Println("\n--- BFS from sample user (depth 2) ---")
	start = time.Now()
	bfsResults, _ := db.BFSCollect(sampleID, 2, graphdb.Outgoing)
	bfsDur := time.Since(start)
	fmt.Printf("  Reached %d nodes in %v\n", len(bfsResults), bfsDur)
	fmt.Printf("  First 5: ")
	for i := 0; i < 5 && i < len(bfsResults); i++ {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s(d=%d)", bfsResults[i].Node.GetString("name"), bfsResults[i].Depth)
	}
	fmt.Println()

	// ── 6. DFS Traversal ─────────────────────────────────────────────
	fmt.Println("\n--- DFS from sample user (depth 2) ---")
	start = time.Now()
	dfsResults, _ := db.DFSCollect(sampleID, 2, graphdb.Outgoing)
	dfsDur := time.Since(start)
	fmt.Printf("  Reached %d nodes in %v\n", len(dfsResults), dfsDur)

	// ── 7. Shortest Path ─────────────────────────────────────────────
	targetIdx := numUsers / 2
	targetID := userIDs[targetIdx]
	targetNode, _ := db.GetNode(targetID)
	targetName := targetNode.GetString("name")
	fmt.Printf("\n--- Shortest path: %s → %s ---\n", sampleName, targetName)
	start = time.Now()
	path, _ := db.ShortestPath(sampleID, targetID)
	spDur := time.Since(start)
	if path != nil {
		fmt.Printf("  Path length: %d hops (found in %v)\n", len(path.Edges), spDur)
		fmt.Printf("  Route: ")
		for i, n := range path.Nodes {
			if i > 0 {
				fmt.Print(" → ")
			}
			fmt.Print(n.GetString("name"))
		}
		fmt.Println()
	} else {
		fmt.Printf("  No path found (%v)\n", spDur)
	}

	// ── 8. Query Builder (Fluent API) ────────────────────────────────
	fmt.Println("\n--- Query Builder: engineers in sample user's 2-hop network ---")
	start = time.Now()
	result, err := db.NewQuery().
		From(sampleID).
		FollowEdge("follows").
		Depth(2).
		Where(func(n *graphdb.Node) bool {
			return n.GetString("role") == "engineer"
		}).
		Execute()
	qDur := time.Since(start)
	if err != nil {
		log.Printf("  Query error: %v", err)
	} else {
		fmt.Printf("  Found %d engineers in %v\n", len(result.Nodes), qDur)
		fmt.Printf("  First 5: ")
		for i := 0; i < 5 && i < len(result.Nodes); i++ {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%s(%s)", result.Nodes[i].GetString("name"), result.Nodes[i].GetString("city"))
		}
		fmt.Println()
	}

	// ── 9. Concurrent Queries ────────────────────────────────────────
	fmt.Println("\n--- Concurrent queries (10 users, depth 2) ---")
	ctx := context.Background()
	start = time.Now()

	cq := db.NewConcurrentQuery()
	for i := 0; i < 10; i++ {
		cq.Add(db.NewQuery().From(userIDs[i*1000]).FollowEdge("follows").Depth(2))
	}

	results, err := cq.Execute(ctx)
	if err != nil {
		log.Printf("  Concurrent query error: %v", err)
	} else {
		elapsed := time.Since(start)
		fmt.Printf("  Executed %d queries concurrently in %v\n", len(results), elapsed)
		for i, r := range results {
			uid := userIDs[i*1000]
			un, _ := db.GetNode(uid)
			fmt.Printf("    %s's network: %d nodes\n", un.GetString("name"), r.Count)
		}
	}

	// ── 10. Connected Components ─────────────────────────────────────
	fmt.Println("\n--- Connected Components ---")
	start = time.Now()
	components, _ := db.ConnectedComponents()
	ccDur := time.Since(start)
	fmt.Printf("  Found %d connected component(s) in %v\n", len(components), ccDur)
	for i, comp := range components {
		fmt.Printf("  Component %d: %d nodes\n", i+1, len(comp))
	}

	// ── 11. Index & Find by Property ─────────────────────────────────
	fmt.Println("\n--- Index lookup ---")
	start = time.Now()
	istanbulNodes, _ := db.FindByProperty("city", "Istanbul")
	fmt.Printf("  People in Istanbul: %d (found in %v)\n", len(istanbulNodes), time.Since(start))

	start = time.Now()
	engineers, _ := db.FindByProperty("role", "engineer")
	fmt.Printf("  Engineers: %d (found in %v)\n", len(engineers), time.Since(start))

	// ── 12. Stats ────────────────────────────────────────────────────
	fmt.Println("\n--- Database stats ---")
	stats, _ := db.Stats()
	fmt.Printf("  Nodes:     %d\n", stats.NodeCount)
	fmt.Printf("  Edges:     %d\n", stats.EdgeCount)
	fmt.Printf("  Shards:    %d\n", stats.ShardCount)
	fmt.Printf("  Disk size: %.2f MB\n", float64(stats.DiskSizeBytes)/1024/1024)

	fmt.Println("\n=== Social Network Example Complete ===")
}
