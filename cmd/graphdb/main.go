package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	graphdb "github.com/emre-savci/graphdb"
)

func main() {
	fmt.Println("=== GraphDB Demo ===")
	fmt.Println()

	// Clean up previous demo data.
	dbPath := "./demo_graph.db"
	os.RemoveAll(dbPath)

	// Open the database with default options.
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

	// ========================================
	// 1. Create Nodes with Arbitrary Fields
	// ========================================
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

	fmt.Printf("Created %d nodes\n", db.NodeCount())

	// ========================================
	// 2. Create Edges (Relationships)
	// ========================================
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

	fmt.Printf("Created %d edges\n", db.EdgeCount())

	// ========================================
	// 3. Query Operations
	// ========================================
	fmt.Println("\n--- Basic queries ---")

	// Get a node
	node, _ := db.GetNode(alice)
	fmt.Printf("Alice: %v\n", node.Props)

	// Who does Alice follow?
	follows, _ := db.NeighborsLabeled(alice, "follows")
	fmt.Printf("Alice follows: ")
	for i, n := range follows {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(n.GetString("name"))
	}
	fmt.Println()

	// Who follows Alice?
	inEdges, _ := db.InEdgesLabeled(alice, "follows")
	fmt.Printf("Alice is followed by: ")
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
	fmt.Printf("Alice consumes: ")
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
	fmt.Printf("Alice degree: out=%d, in=%d\n", outDeg, inDeg)

	// ========================================
	// 4. BFS Traversal
	// ========================================
	fmt.Println("\n--- BFS from Alice (depth 2) ---")
	bfsResults, _ := db.BFSCollect(alice, 2, graphdb.Outgoing)
	for _, r := range bfsResults {
		fmt.Printf("  depth=%d: %s (ID=%d)\n", r.Depth, r.Node.GetString("name"), r.Node.ID)
	}

	// ========================================
	// 5. DFS Traversal
	// ========================================
	fmt.Println("\n--- DFS from Alice (depth 2) ---")
	dfsResults, _ := db.DFSCollect(alice, 2, graphdb.Outgoing)
	for _, r := range dfsResults {
		fmt.Printf("  depth=%d: %s (ID=%d)\n", r.Depth, r.Node.GetString("name"), r.Node.ID)
	}

	// ========================================
	// 6. Shortest Path
	// ========================================
	fmt.Println("\n--- Shortest path: Diana -> Eve ---")
	path, _ := db.ShortestPath(diana, eve)
	if path != nil {
		fmt.Printf("  Path length: %d hops\n", len(path.Edges))
		fmt.Printf("  Route: ")
		for i, n := range path.Nodes {
			if i > 0 {
				fmt.Printf(" -> ")
			}
			fmt.Print(n.GetString("name"))
		}
		fmt.Println()
		for _, e := range path.Edges {
			fmt.Printf("    %s\n", e.String())
		}
	}

	// ========================================
	// 7. Query Builder (Fluent API)
	// ========================================
	fmt.Println("\n--- Query: Find engineers that Alice's network consumes topics ---")
	result, err := db.NewQuery().
		From(alice).
		FollowEdge("follows").
		Depth(2).
		Where(func(n *graphdb.Node) bool {
			return n.GetString("role") == "engineer" || n.GetString("role") == "data scientist"
		}).
		Execute()
	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		for _, n := range result.Nodes {
			fmt.Printf("  %s (%s)\n", n.GetString("name"), n.GetString("role"))
		}
	}

	// ========================================
	// 8. Concurrent Queries
	// ========================================
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
		log.Printf("Concurrent query error: %v", err)
	} else {
		elapsed := time.Since(start)
		fmt.Printf("  Executed %d queries concurrently in %v\n", len(results), elapsed)
		names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
		for i, r := range results {
			fmt.Printf("  %s's network: %d nodes\n", names[i], r.Count)
		}
	}

	// ========================================
	// 9. Connected Components
	// ========================================
	fmt.Println("\n--- Connected Components ---")
	components, _ := db.ConnectedComponents()
	fmt.Printf("  Found %d connected component(s)\n", len(components))
	for i, comp := range components {
		fmt.Printf("  Component %d: %d nodes\n", i+1, len(comp))
	}

	// ========================================
	// 10. Index & Find by Property
	// ========================================
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

	// ========================================
	// 11. Cypher Query Examples
	// ========================================
	fmt.Println("\n--- Cypher queries ---")

	// 11a. All nodes
	fmt.Println("\n  [MATCH (n) RETURN n] — all nodes:")
	res, err := db.Cypher(`MATCH (n) RETURN n`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		fmt.Printf("    → %d rows returned\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("      %s (ID=%d)\n", n.GetString("name"), n.ID)
			}
		}
	}

	// 11b. Property filter
	fmt.Println("\n  [MATCH (n {name: \"Alice\"}) RETURN n] — property filter:")
	res, err = db.Cypher(`MATCH (n {name: "Alice"}) RETURN n`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("    → Found: %v\n", n.Props)
			}
		}
	}

	// 11c. WHERE clause with comparison
	fmt.Println("\n  [MATCH (n) WHERE n.age > 30 RETURN n] — WHERE clause:")
	res, err = db.Cypher(`MATCH (n) WHERE n.age > 30 RETURN n`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		fmt.Printf("    → %d nodes with age > 30:\n", len(res.Rows))
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("      %s (age=%v)\n", n.GetString("name"), n.Props["age"])
			}
		}
	}

	// 11d. 1-hop pattern — who does someone follow?
	fmt.Println("\n  [MATCH (a)-[:follows]->(b) RETURN a, b] — 1-hop pattern:")
	res, err = db.Cypher(`MATCH (a)-[:follows]->(b) RETURN a, b`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		fmt.Printf("    → %d follow relationships:\n", len(res.Rows))
		for _, row := range res.Rows {
			a := row["a"].(*graphdb.Node)
			b := row["b"].(*graphdb.Node)
			fmt.Printf("      %s --> %s\n", a.GetString("name"), b.GetString("name"))
		}
	}

	// 11e. Filtered traversal with property projection
	fmt.Println("\n  [MATCH (a {name: \"Alice\"})-[:follows]->(b) RETURN b.name] — filtered traversal:")
	res, err = db.Cypher(`MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		for _, row := range res.Rows {
			fmt.Printf("    → Alice follows: %v\n", row["b.name"])
		}
	}

	// 11f. Variable-length path
	fmt.Println("\n  [MATCH (a {name: \"Alice\"})-[:follows*1..2]->(b) RETURN b] — variable-length path:")
	res, err = db.Cypher(`MATCH (a {name: "Alice"})-[:follows*1..2]->(b) RETURN b`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		fmt.Printf("    → %d nodes reachable in 1-2 hops:\n", len(res.Rows))
		for _, row := range res.Rows {
			if b, ok := row["b"].(*graphdb.Node); ok {
				fmt.Printf("      %s\n", b.GetString("name"))
			}
		}
	}

	// 11g. Any edge type with type() function
	fmt.Println("\n  [MATCH (a {name: \"Alice\"})-[r]->(b) RETURN type(r), b.name] — any edge type:")
	res, err = db.Cypher(`MATCH (a {name: "Alice"})-[r]->(b) RETURN type(r), b.name`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		for _, row := range res.Rows {
			fmt.Printf("    → Alice -[%v]-> %v\n", row["type(r)"], row["b.name"])
		}
	}

	// 11h. ORDER BY + LIMIT
	fmt.Println("\n  [MATCH (n) WHERE n.age > 0 RETURN n ORDER BY n.age DESC LIMIT 3] — ordered + limited:")
	res, err = db.Cypher(`MATCH (n) WHERE n.age > 0 RETURN n ORDER BY n.age DESC LIMIT 3`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		for _, row := range res.Rows {
			if n, ok := row["n"].(*graphdb.Node); ok {
				fmt.Printf("    → %s (age=%v)\n", n.GetString("name"), n.Props["age"])
			}
		}
	}

	// 11i. Alias in RETURN
	fmt.Println("\n  [MATCH (n {name: \"Bob\"}) RETURN n.name AS person, n.role AS job] — aliases:")
	res, err = db.Cypher(`MATCH (n {name: "Bob"}) RETURN n.name AS person, n.role AS job`)
	if err != nil {
		log.Printf("Cypher error: %v", err)
	} else {
		fmt.Printf("    Columns: %v\n", res.Columns)
		for _, row := range res.Rows {
			fmt.Printf("    → person=%v, job=%v\n", row["person"], row["job"])
		}
	}

	// ========================================
	// 12. Cypher Performance Tests
	// ========================================
	fmt.Println("\n--- Cypher performance ---")

	// 12a. Simple scan: MATCH (n) RETURN n
	start = time.Now()
	res, _ = db.Cypher(`MATCH (n) RETURN n`)
	fmt.Printf("  MATCH (n) RETURN n            → %d rows in %v\n", len(res.Rows), time.Since(start))

	// 12b. Property filter on small dataset
	start = time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (n {name: "Alice"}) RETURN n`)
	}
	fmt.Printf("  Property filter × 1,000       → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	// 12c. WHERE clause × 1,000
	start = time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (n) WHERE n.age > 30 RETURN n`)
	}
	fmt.Printf("  WHERE clause × 1,000          → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	// 12d. 1-hop pattern × 1,000
	start = time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b`)
	}
	fmt.Printf("  1-hop pattern × 1,000         → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	// 12e. Variable-length path × 1,000
	start = time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (a {name: "Alice"})-[:follows*1..3]->(b) RETURN b`)
	}
	fmt.Printf("  Var-length path × 1,000       → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	// 12f. Any-edge type × 1,000
	start = time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (a {name: "Alice"})-[r]->(b) RETURN type(r), b`)
	}
	fmt.Printf("  Any-edge + type() × 1,000     → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	// 12g. Concurrent Cypher queries
	fmt.Println("\n  Concurrent Cypher benchmark:")
	queries := []string{
		`MATCH (n {name: "Alice"}) RETURN n`,
		`MATCH (a)-[:follows]->(b) RETURN a, b`,
		`MATCH (n) WHERE n.age > 25 RETURN n`,
		`MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`,
		`MATCH (a {name: "Alice"})-[:follows*1..2]->(b) RETURN b`,
		`MATCH (a {name: "Alice"})-[r]->(b) RETURN type(r), b`,
	}

	const concurrentRuns = 500
	start = time.Now()
	done := make(chan struct{}, len(queries)*concurrentRuns)
	for _, q := range queries {
		for j := 0; j < concurrentRuns; j++ {
			go func(query string) {
				db.Cypher(query)
				done <- struct{}{}
			}(q)
		}
	}
	for i := 0; i < len(queries)*concurrentRuns; i++ {
		<-done
	}
	totalQueries := len(queries) * concurrentRuns
	elapsed := time.Since(start)
	fmt.Printf("    %d concurrent queries in %v  (%.0f queries/sec)\n",
		totalQueries, elapsed, float64(totalQueries)/elapsed.Seconds())

	// ========================================
	// 13. Batch Insert — Realistic Social Network Data
	// ========================================
	fmt.Println("\n--- Batch insert: realistic social network data ---")

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
	topics := []string{
		"Go Programming", "Graph Theory", "Kubernetes", "Machine Learning",
		"Distributed Systems", "React", "Rust", "PostgreSQL", "Redis", "Kafka",
	}
	edgeTypes := []string{"follows", "consumes", "manages", "mentors", "collaborates_with"}

	rng := rand.New(rand.NewSource(42))

	batchSize := 100_000
	fmt.Printf("  Generating %d person nodes...\n", batchSize)

	// --- Person nodes ---
	propsList := make([]graphdb.Props, batchSize)
	for i := 0; i < batchSize; i++ {
		propsList[i] = graphdb.Props{
			"name": fmt.Sprintf("%s %s",
				firstNames[rng.Intn(len(firstNames))],
				lastNames[rng.Intn(len(lastNames))]),
			"age":  float64(18 + rng.Intn(50)), // 18–67
			"city": cities[rng.Intn(len(cities))],
			"role": roles[rng.Intn(len(roles))],
		}
	}

	start = time.Now()
	personIDs, err := db.AddNodeBatch(propsList)
	if err != nil {
		log.Fatalf("Batch add persons failed: %v", err)
	}
	fmt.Printf("  Inserted %d person nodes in %v (%.0f nodes/sec)\n",
		batchSize, time.Since(start),
		float64(batchSize)/time.Since(start).Seconds())

	// --- Topic nodes ---
	topicProps := make([]graphdb.Props, len(topics))
	for i, t := range topics {
		topicProps[i] = graphdb.Props{
			"name":       t,
			"type":       "topic",
			"difficulty": []string{"beginner", "intermediate", "advanced"}[rng.Intn(3)],
		}
	}
	topicIDs, _ := db.AddNodeBatch(topicProps)
	fmt.Printf("  Inserted %d topic nodes\n", len(topicIDs))

	// --- Edges: social + knowledge ---
	edgesPerPerson := 5 // each person gets ~5 random edges
	totalEdges := batchSize * edgesPerPerson
	fmt.Printf("  Generating %d edges (%d per person)...\n", totalEdges, edgesPerPerson)

	edgeBatch := make([]graphdb.Edge, 0, totalEdges)
	for i := 0; i < batchSize; i++ {
		from := personIDs[i]

		// 2-3 "follows" edges to random persons
		numFollows := 2 + rng.Intn(2)
		for f := 0; f < numFollows; f++ {
			to := personIDs[rng.Intn(batchSize)]
			if to == from {
				continue
			}
			edgeBatch = append(edgeBatch, graphdb.Edge{
				From:  from,
				To:    to,
				Label: "follows",
				Props: graphdb.Props{"since": fmt.Sprintf("%d", 2020+rng.Intn(6))},
			})
		}

		// 1 "consumes" edge to a random topic
		edgeBatch = append(edgeBatch, graphdb.Edge{
			From:  from,
			To:    topicIDs[rng.Intn(len(topicIDs))],
			Label: "consumes",
			Props: graphdb.Props{"progress": fmt.Sprintf("%d%%", rng.Intn(100))},
		})

		// 1 random relationship (manages/mentors/collaborates_with)
		to := personIDs[rng.Intn(batchSize)]
		if to != from {
			lbl := edgeTypes[2+rng.Intn(3)] // manages, mentors, collaborates_with
			edgeBatch = append(edgeBatch, graphdb.Edge{
				From: from, To: to, Label: lbl,
			})
		}
	}

	start = time.Now()
	_, err = db.AddEdgeBatch(edgeBatch)
	if err != nil {
		log.Fatalf("Batch edge add failed: %v", err)
	}
	fmt.Printf("  Inserted %d edges in %v (%.0f edges/sec)\n",
		len(edgeBatch), time.Since(start),
		float64(len(edgeBatch))/time.Since(start).Seconds())

	// Build indexes on properties used by Cypher queries.
	fmt.Println("\n  Building indexes for Cypher query optimizer...")
	start = time.Now()
	for _, prop := range []string{"name", "age", "city", "role"} {
		db.CreateIndex(prop)
	}
	fmt.Printf("  Indexes built in %v  (name, age, city, role)\n", time.Since(start))

	// ========================================
	// 14. Cypher Performance Against Large Dataset
	// ========================================
	fmt.Println("\n--- Cypher performance on large dataset (with indexes) ---")

	totalNodes := db.NodeCount()
	totalEdgeCount := db.EdgeCount()
	fmt.Printf("  Dataset: %d nodes, %d edges\n\n", totalNodes, totalEdgeCount)

	// Pick a random person to use in filtered queries
	sampleID := personIDs[rng.Intn(batchSize)]
	sampleNode, _ := db.GetNode(sampleID)
	sampleName := sampleNode.GetString("name")
	fmt.Printf("  Sample node: %q (ID=%d, age=%v, city=%s)\n\n",
		sampleName, sampleID, sampleNode.Props["age"], sampleNode.GetString("city"))

	// 14a. Full scan: MATCH (n) RETURN n
	start = time.Now()
	res, _ = db.Cypher(`MATCH (n) RETURN n`)
	fmt.Printf("  MATCH (n) RETURN n               → %d rows in %v\n",
		len(res.Rows), time.Since(start))

	// 14b. Property filter by name
	q14b := fmt.Sprintf(`MATCH (n {name: "%s"}) RETURN n`, sampleName)
	start = time.Now()
	res, _ = db.Cypher(q14b)
	fmt.Printf("  Property filter {name: %q}  → %d rows in %v\n",
		sampleName, len(res.Rows), time.Since(start))

	// 14c. WHERE clause — engineers over 40
	start = time.Now()
	res, _ = db.Cypher(`MATCH (n) WHERE n.age > 40 RETURN n`)
	fmt.Printf("  WHERE n.age > 40                 → %d rows in %v\n",
		len(res.Rows), time.Since(start))

	// 14d. WHERE clause — city filter
	start = time.Now()
	res, _ = db.Cypher(`MATCH (n) WHERE n.city = "Istanbul" RETURN n`)
	fmt.Printf("  WHERE n.city = \"Istanbul\"         → %d rows in %v\n",
		len(res.Rows), time.Since(start))

	// 14e. 1-hop follows
	start = time.Now()
	res, _ = db.Cypher(`MATCH (a)-[:follows]->(b) RETURN a, b`)
	fmt.Printf("  All follows (1-hop)              → %d rows in %v\n",
		len(res.Rows), time.Since(start))

	// 14f. Filtered 1-hop: who does sample person follow?
	q14f := fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows]->(b) RETURN b.name`, sampleName)
	start = time.Now()
	res, _ = db.Cypher(q14f)
	fmt.Printf("  %q follows → → %d people in %v\n",
		sampleName, len(res.Rows), time.Since(start))

	// 14g. Variable-length path 1..3 from sample
	q14g := fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows*1..3]->(b) RETURN b`, sampleName)
	start = time.Now()
	res, _ = db.Cypher(q14g)
	fmt.Printf("  Var-length follows*1..3          → %d rows in %v\n",
		len(res.Rows), time.Since(start))

	// 14h. Any edge type from sample
	q14h := fmt.Sprintf(`MATCH (a {name: "%s"})-[r]->(b) RETURN type(r), b.name`, sampleName)
	start = time.Now()
	res, _ = db.Cypher(q14h)
	fmt.Printf("  Any-edge + type() from sample    → %d rows in %v\n",
		len(res.Rows), time.Since(start))

	// 14i. ORDER BY + LIMIT on large set
	start = time.Now()
	res, _ = db.Cypher(`MATCH (n) WHERE n.age > 0 RETURN n ORDER BY n.age DESC LIMIT 10`)
	fmt.Printf("  ORDER BY age DESC LIMIT 10       → %d rows in %v\n",
		len(res.Rows), time.Since(start))
	for _, row := range res.Rows {
		if n, ok := row["n"].(*graphdb.Node); ok {
			fmt.Printf("    %s (age=%v, city=%s)\n",
				n.GetString("name"), n.Props["age"], n.GetString("city"))
		}
	}

	// 14j. Repeated query benchmark (1,000 iterations) on large dataset
	fmt.Println("\n  Repeated benchmarks (×1,000) on large dataset:")

	benchmarks := []struct {
		label string
		query string
	}{
		{"Property filter (name)", fmt.Sprintf(`MATCH (n {name: "%s"}) RETURN n`, sampleName)},
		{"WHERE age > 50", `MATCH (n) WHERE n.age > 50 RETURN n`},
		{"1-hop follows (sample)", fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows]->(b) RETURN b`, sampleName)},
		{"Var-length *1..2 (sample)", fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows*1..2]->(b) RETURN b`, sampleName)},
		{"Any-edge type() (sample)", fmt.Sprintf(`MATCH (a {name: "%s"})-[r]->(b) RETURN type(r), b`, sampleName)},
	}

	for _, bm := range benchmarks {
		start = time.Now()
		for i := 0; i < 1000; i++ {
			db.Cypher(bm.query)
		}
		dur := time.Since(start)
		fmt.Printf("    %-35s → %v  (%.1f µs/query)\n",
			bm.label, dur, float64(dur.Microseconds())/1000.0)
	}

	// 14k. Concurrent Cypher on large dataset
	fmt.Println("\n  Concurrent Cypher on large dataset:")
	concQueries := []string{
		fmt.Sprintf(`MATCH (n {name: "%s"}) RETURN n`, sampleName),
		`MATCH (n) WHERE n.age > 50 RETURN n`,
		fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows]->(b) RETURN b.name`, sampleName),
		fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows*1..2]->(b) RETURN b`, sampleName),
		fmt.Sprintf(`MATCH (a {name: "%s"})-[r]->(b) RETURN type(r), b`, sampleName),
	}

	const concRuns = 200
	start = time.Now()
	done = make(chan struct{}, len(concQueries)*concRuns)
	for _, cq := range concQueries {
		for j := 0; j < concRuns; j++ {
			go func(query string) {
				db.Cypher(query)
				done <- struct{}{}
			}(cq)
		}
	}
	for i := 0; i < len(concQueries)*concRuns; i++ {
		<-done
	}
	totalQ := len(concQueries) * concRuns
	dur := time.Since(start)
	fmt.Printf("    %d concurrent queries in %v  (%.0f queries/sec)\n",
		totalQ, dur, float64(totalQ)/dur.Seconds())

	// ========================================
	// 15. Stats
	// ========================================
	fmt.Println("\n--- Database stats ---")
	stats, _ := db.Stats()
	fmt.Printf("  Nodes:      %d\n", stats.NodeCount)
	fmt.Printf("  Edges:      %d\n", stats.EdgeCount)
	fmt.Printf("  Shards:     %d\n", stats.ShardCount)
	fmt.Printf("  Disk size:  %.2f MB\n", float64(stats.DiskSizeBytes)/1024/1024)

	_ = golang
	_ = graphTheory
	_ = kubernetes

	fmt.Println("\n=== Demo Complete ===")
}
