// Example: Data Integrity — CRC32 Checksums + VerifyIntegrity
//
// Demonstrates:
//   - CRC32 (Castagnoli) checksums on all node/edge data
//   - VerifyIntegrity() full-scan that checks every record across all shards
//   - How corruption would be detected (simulated via log output)
//
// Run:
//
//	go run ./examples/integrity/
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
	fmt.Println("=== Data Integrity (CRC32 Checksums) Example ===")
	fmt.Println()

	// ── Setup ────────────────────────────────────────────────────────
	dbPath := "./integrity_example.db"
	os.RemoveAll(dbPath)

	opts := graphdb.DefaultOptions()
	opts.ShardCount = 2 // use 2 shards to show cross-shard verification
	db, err := graphdb.Open(dbPath, opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbPath)
	}()

	// ── 1. Create Data ──────────────────────────────────────────────
	fmt.Println("--- Creating dataset ---")

	// Batch insert nodes.
	propsList := make([]graphdb.Props, 1000)
	for i := 0; i < 1000; i++ {
		propsList[i] = graphdb.Props{
			"name":  fmt.Sprintf("User_%04d", i),
			"email": fmt.Sprintf("user%04d@example.com", i),
			"age":   float64(18 + i%50),
			"score": float64(i * 17 % 100),
		}
	}
	ids, err := db.AddNodeBatch(propsList)
	if err != nil {
		log.Fatalf("Batch insert failed: %v", err)
	}
	fmt.Printf("  Inserted %d nodes\n", len(ids))

	// Add edges.
	edgeBatch := make([]graphdb.Edge, 0, 2000)
	for i := 0; i < 1000; i++ {
		// Each node connects to two others.
		edgeBatch = append(edgeBatch, graphdb.Edge{
			From:  ids[i],
			To:    ids[(i+1)%1000],
			Label: "FOLLOWS",
			Props: graphdb.Props{"weight": float64(i%10 + 1)},
		})
		edgeBatch = append(edgeBatch, graphdb.Edge{
			From:  ids[i],
			To:    ids[(i+7)%1000],
			Label: "KNOWS",
		})
	}
	edgeIDs, err := db.AddEdgeBatch(edgeBatch)
	if err != nil {
		log.Fatalf("Batch edge insert failed: %v", err)
	}
	fmt.Printf("  Inserted %d edges\n", len(edgeIDs))
	fmt.Printf("  Total: %d nodes, %d edges across %d shards\n",
		db.NodeCount(), db.EdgeCount(), opts.ShardCount)

	// ── 2. Verify Integrity ─────────────────────────────────────────
	fmt.Println("\n--- Running VerifyIntegrity ---")
	start := time.Now()
	report, err := db.VerifyIntegrity()
	elapsed := time.Since(start)
	if err != nil {
		log.Fatalf("VerifyIntegrity failed: %v", err)
	}

	fmt.Printf("  Nodes checked: %d\n", report.NodesChecked)
	fmt.Printf("  Edges checked: %d\n", report.EdgesChecked)
	fmt.Printf("  Time: %v\n", elapsed)

	if report.OK() {
		fmt.Println("  Result: ALL DATA INTACT — no corruption detected")
	} else {
		fmt.Printf("  Result: CORRUPTION FOUND — %d errors\n", len(report.Errors))
		for _, e := range report.Errors {
			fmt.Printf("    %s\n", e.Error())
		}
	}

	// ── 3. Read-path verification ───────────────────────────────────
	// Every GetNode / GetEdge call also verifies CRC32 transparently.
	fmt.Println("\n--- Read-path checksum verification ---")
	fmt.Println("  Every GetNode/GetEdge call automatically verifies CRC32.")
	fmt.Println("  Corrupted data would return an error immediately.")

	node, err := db.GetNode(ids[0])
	if err != nil {
		fmt.Printf("  GetNode failed (corruption?): %v\n", err)
	} else {
		fmt.Printf("  GetNode(%d) → %s (%s) — checksum OK\n",
			ids[0], node.GetString("name"), node.GetString("email"))
	}

	edge, err := db.GetEdge(edgeIDs[0])
	if err != nil {
		fmt.Printf("  GetEdge failed (corruption?): %v\n", err)
	} else {
		fmt.Printf("  GetEdge(%d) → %d-[%s]->%d — checksum OK\n",
			edgeIDs[0], edge.From, edge.Label, edge.To)
	}

	// ── 4. Update and re-verify ─────────────────────────────────────
	// After mutations, new data is written with fresh CRC32 checksums.
	fmt.Println("\n--- Mutate and re-verify ---")
	db.UpdateNode(ids[0], graphdb.Props{"name": "Updated_User_0000", "verified": true})
	db.UpdateNode(ids[500], graphdb.Props{"name": "Updated_User_0500", "verified": true})

	start = time.Now()
	report, err = db.VerifyIntegrity()
	elapsed = time.Since(start)
	if err != nil {
		log.Fatalf("VerifyIntegrity failed: %v", err)
	}

	fmt.Printf("  Re-verified %d nodes + %d edges in %v\n",
		report.NodesChecked, report.EdgesChecked, elapsed)
	if report.OK() {
		fmt.Println("  Result: ALL DATA INTACT after mutations")
	} else {
		fmt.Printf("  Result: %d errors found\n", len(report.Errors))
	}

	// Read updated node to confirm.
	updated, _ := db.GetNode(ids[0])
	fmt.Printf("  Updated node: %s (verified=%v)\n",
		updated.GetString("name"), updated.Props["verified"])

	fmt.Println("\n=== Data Integrity Example Complete ===")
}
