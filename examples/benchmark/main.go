// Example: Performance Benchmarks
//
// Demonstrates batch inserting a large social-network dataset (100K nodes,
// ~450K edges), building indexes, and benchmarking Cypher queries against it.
//
// Run:
//
//	go run ./examples/benchmark/
package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	graphdb "github.com/emre-savci/graphdb"
)

func main() {
	fmt.Println("=== Benchmark Example ===")
	fmt.Println()

	// ── Open database ────────────────────────────────────────────────
	dbPath := "./benchmark_example.db"
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

	// ── Small-dataset Cypher warmup ──────────────────────────────────
	fmt.Println("--- Small-dataset warmup ---")

	alice, _ := db.AddNode(graphdb.Props{"name": "Alice", "age": 30, "city": "Istanbul"})
	bob, _ := db.AddNode(graphdb.Props{"name": "Bob", "age": 25, "city": "Ankara"})
	db.AddEdge(alice, bob, "follows", graphdb.Props{"since": "2023"})
	db.CreateIndex("name")

	start := time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (n {name: "Alice"}) RETURN n`)
	}
	fmt.Printf("  Property filter × 1,000       → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	start = time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (n) WHERE n.age > 25 RETURN n`)
	}
	fmt.Printf("  WHERE clause × 1,000          → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	start = time.Now()
	for i := 0; i < 1000; i++ {
		db.Cypher(`MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b`)
	}
	fmt.Printf("  1-hop pattern × 1,000         → %v  (%.1f µs/query)\n",
		time.Since(start), float64(time.Since(start).Microseconds())/1000.0)

	// ── Batch insert — realistic social network ──────────────────────
	fmt.Println("\n--- Batch insert: 100K persons + 450K edges ---")

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

	const batchSize = 100_000

	// Person nodes
	propsList := make([]graphdb.Props, batchSize)
	for i := 0; i < batchSize; i++ {
		propsList[i] = graphdb.Props{
			"name": fmt.Sprintf("%s %s",
				firstNames[rng.Intn(len(firstNames))],
				lastNames[rng.Intn(len(lastNames))]),
			"age":  float64(18 + rng.Intn(50)),
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

	// Topic nodes
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

	// Edges
	edgesPerPerson := 5
	totalEdges := batchSize * edgesPerPerson
	fmt.Printf("  Generating %d edges (%d per person)...\n", totalEdges, edgesPerPerson)

	edgeBatch := make([]graphdb.Edge, 0, totalEdges)
	for i := 0; i < batchSize; i++ {
		from := personIDs[i]

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

		edgeBatch = append(edgeBatch, graphdb.Edge{
			From:  from,
			To:    topicIDs[rng.Intn(len(topicIDs))],
			Label: "consumes",
			Props: graphdb.Props{"progress": fmt.Sprintf("%d%%", rng.Intn(100))},
		})

		to := personIDs[rng.Intn(batchSize)]
		if to != from {
			lbl := edgeTypes[2+rng.Intn(3)]
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

	// Build indexes after batch insert.
	fmt.Println("\n  Building indexes...")
	start = time.Now()
	for _, prop := range []string{"name", "age", "city", "role"} {
		db.CreateIndex(prop)
	}
	fmt.Printf("  Indexes built in %v  (name, age, city, role)\n", time.Since(start))

	// Auto-index verification
	testID, _ := db.AddNode(graphdb.Props{"name": "IndexTest", "age": float64(99), "city": "TestCity"})
	testResult, _ := db.FindByProperty("name", "IndexTest")
	fmt.Printf("  Auto-index verify: AddNode → FindByProperty = %d node(s) ✓\n", len(testResult))
	db.DeleteNode(testID)

	// ── Cypher on large dataset ──────────────────────────────────────
	fmt.Println("\n--- Cypher performance on large dataset ---")

	totalNodes := db.NodeCount()
	totalEdgeCount := db.EdgeCount()
	fmt.Printf("  Dataset: %d nodes, %d edges\n\n", totalNodes, totalEdgeCount)

	sampleID := personIDs[rng.Intn(batchSize)]
	sampleNode, _ := db.GetNode(sampleID)
	sampleName := sampleNode.GetString("name")
	fmt.Printf("  Sample: %q (ID=%d, age=%v, city=%s)\n\n",
		sampleName, sampleID, sampleNode.Props["age"], sampleNode.GetString("city"))

	// Single-query benchmarks
	bench := func(label, query string) {
		start = time.Now()
		res, _ := db.Cypher(query)
		fmt.Printf("  %-40s → %d rows in %v\n", label, len(res.Rows), time.Since(start))
	}

	bench("MATCH (n) RETURN n", `MATCH (n) RETURN n`)
	bench(fmt.Sprintf("Property {name: %q}", sampleName),
		fmt.Sprintf(`MATCH (n {name: "%s"}) RETURN n`, sampleName))
	bench("WHERE n.age > 40",
		`MATCH (n) WHERE n.age > 40 RETURN n`)
	bench(`WHERE n.city = "Istanbul"`,
		`MATCH (n) WHERE n.city = "Istanbul" RETURN n`)
	bench("All follows (edge-type scan)",
		`MATCH (a)-[:follows]->(b) RETURN a, b`)
	bench(fmt.Sprintf("%q follows → (1-hop)", sampleName),
		fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows]->(b) RETURN b.name`, sampleName))
	bench("Var-length *1..3 from sample",
		fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows*1..3]->(b) RETURN b`, sampleName))
	bench("Any-edge + type() from sample",
		fmt.Sprintf(`MATCH (a {name: "%s"})-[r]->(b) RETURN type(r), b.name`, sampleName))

	// ORDER BY + LIMIT (heap-based top-K)
	start = time.Now()
	res, _ := db.Cypher(`MATCH (n) WHERE n.age > 0 RETURN n ORDER BY n.age DESC LIMIT 10`)
	fmt.Printf("  %-40s → %d rows in %v\n", "ORDER BY + LIMIT (heap top-K)", len(res.Rows), time.Since(start))
	for _, row := range res.Rows {
		if n, ok := row["n"].(*graphdb.Node); ok {
			fmt.Printf("    %s (age=%v, city=%s)\n",
				n.GetString("name"), n.Props["age"], n.GetString("city"))
		}
	}

	// ── Repeated benchmarks with PrepareCypher ───────────────────────
	fmt.Println("\n--- Repeated benchmarks (×1,000) with PrepareCypher ---")

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
		pq, err := db.PrepareCypher(bm.query)
		if err != nil {
			log.Printf("PrepareCypher error: %v", err)
			continue
		}
		start = time.Now()
		for i := 0; i < 1000; i++ {
			db.ExecutePrepared(pq)
		}
		dur := time.Since(start)
		fmt.Printf("  %-35s → %v  (%.1f µs/query)\n",
			bm.label, dur, float64(dur.Microseconds())/1000.0)
	}

	// Cache comparison
	fmt.Println("\n--- Cache comparison (property filter ×10,000) ---")
	cacheQ := fmt.Sprintf(`MATCH (n {name: "%s"}) RETURN n`, sampleName)

	start = time.Now()
	for i := 0; i < 10_000; i++ {
		db.Cypher(cacheQ)
	}
	cypherDur := time.Since(start)
	fmt.Printf("  db.Cypher()          → %v  (%.1f µs/query)\n",
		cypherDur, float64(cypherDur.Microseconds())/10_000.0)

	pqCache, _ := db.PrepareCypher(cacheQ)
	start = time.Now()
	for i := 0; i < 10_000; i++ {
		db.ExecutePrepared(pqCache)
	}
	prepDur := time.Since(start)
	fmt.Printf("  db.ExecutePrepared() → %v  (%.1f µs/query)\n",
		prepDur, float64(prepDur.Microseconds())/10_000.0)

	// ── Concurrent Cypher ────────────────────────────────────────────
	fmt.Println("\n--- Concurrent Cypher on large dataset ---")

	concQueries := []string{
		fmt.Sprintf(`MATCH (n {name: "%s"}) RETURN n`, sampleName),
		`MATCH (n) WHERE n.age > 50 RETURN n`,
		fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows]->(b) RETURN b.name`, sampleName),
		fmt.Sprintf(`MATCH (a {name: "%s"})-[:follows*1..2]->(b) RETURN b`, sampleName),
		fmt.Sprintf(`MATCH (a {name: "%s"})-[r]->(b) RETURN type(r), b`, sampleName),
	}

	const concRuns = 200
	start = time.Now()
	done := make(chan struct{}, len(concQueries)*concRuns)
	for _, q := range concQueries {
		for j := 0; j < concRuns; j++ {
			go func(query string) {
				db.Cypher(query)
				done <- struct{}{}
			}(q)
		}
	}
	for i := 0; i < len(concQueries)*concRuns; i++ {
		<-done
	}
	totalQ := len(concQueries) * concRuns
	dur := time.Since(start)
	fmt.Printf("  %d concurrent queries in %v  (%.0f queries/sec)\n",
		totalQ, dur, float64(totalQ)/dur.Seconds())

	// ── Stats ────────────────────────────────────────────────────────
	fmt.Println("\n--- Database stats ---")
	stats, _ := db.Stats()
	fmt.Printf("  Nodes:     %d\n", stats.NodeCount)
	fmt.Printf("  Edges:     %d\n", stats.EdgeCount)
	fmt.Printf("  Shards:    %d\n", stats.ShardCount)
	fmt.Printf("  Disk size: %.2f MB\n", float64(stats.DiskSizeBytes)/1024/1024)

	fmt.Println("\n=== Benchmark Example Complete ===")
}
