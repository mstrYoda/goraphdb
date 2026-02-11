package graphdb

import (
	"testing"
)

func TestBloomFilter_BasicAddAndTest(t *testing.T) {
	bf := newBloomFilter(1000)

	// Add some edges.
	bf.Add(1, 2)
	bf.Add(3, 4)
	bf.Add(5, 6)

	// Test positive cases (must return true — no false negatives).
	if !bf.Test(1, 2) {
		t.Error("expected bloom filter to return true for (1,2)")
	}
	if !bf.Test(3, 4) {
		t.Error("expected bloom filter to return true for (3,4)")
	}
	if !bf.Test(5, 6) {
		t.Error("expected bloom filter to return true for (5,6)")
	}

	// Test negative cases — high probability of returning false.
	// We check several non-existent edges; if ALL return true, something is wrong.
	falsePositives := 0
	for i := NodeID(100); i < 200; i++ {
		if bf.Test(i, i+1000) {
			falsePositives++
		}
	}
	// With 1000 capacity and only 3 elements, FPR should be near zero.
	if falsePositives > 10 {
		t.Errorf("too many false positives: %d out of 100", falsePositives)
	}
}

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	bf := newBloomFilter(10000)

	// Add 5000 edges.
	for i := NodeID(1); i <= 5000; i++ {
		bf.Add(i, i+10000)
	}

	// Verify zero false negatives.
	for i := NodeID(1); i <= 5000; i++ {
		if !bf.Test(i, i+10000) {
			t.Fatalf("false negative for edge (%d, %d)", i, i+10000)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := uint64(10000)
	bf := newBloomFilter(n)

	// Insert n edges.
	for i := NodeID(1); i <= NodeID(n); i++ {
		bf.Add(i, i+100000)
	}

	// Test 10000 edges that were NOT inserted.
	falsePositives := 0
	tests := 10000
	for i := NodeID(100001); i < NodeID(100001+tests); i++ {
		if bf.Test(i, i+200000) {
			falsePositives++
		}
	}

	fpr := float64(falsePositives) / float64(tests)
	t.Logf("False positive rate: %.2f%% (%d/%d)", fpr*100, falsePositives, tests)

	// With ~10 bits per element and k=4, FPR should be well under 5%.
	if fpr > 0.05 {
		t.Errorf("false positive rate too high: %.2f%%", fpr*100)
	}
}

func TestHasEdge_BloomFilterIntegration(t *testing.T) {
	db := testDB(t)

	// Create two nodes and an edge.
	alice, _ := db.AddNode(Props{"name": "Alice"})
	bob, _ := db.AddNode(Props{"name": "Bob"})
	charlie, _ := db.AddNode(Props{"name": "Charlie"})

	_, err := db.AddEdge(alice, bob, "knows", nil)
	if err != nil {
		t.Fatal(err)
	}

	// HasEdge(alice, bob) should return true.
	has, err := db.HasEdge(alice, bob)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("expected HasEdge(alice, bob) = true")
	}

	// HasEdge(alice, charlie) should return false.
	has, err = db.HasEdge(alice, charlie)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("expected HasEdge(alice, charlie) = false")
	}

	// Verify bloom filter was effective (metrics should show negatives).
	if db.metrics != nil {
		negatives := db.metrics.BloomNegatives.Load()
		// The alice→charlie check should have been caught by bloom filter.
		if negatives < 1 {
			t.Logf("bloom negatives: %d (may not fire if hash collision)", negatives)
		}
	}
}

func TestBloomFilter_RebuiltOnOpen(t *testing.T) {
	dir := t.TempDir()

	// Open, add edges, close.
	db1, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := db1.AddNode(Props{"name": "A"})
	b, _ := db1.AddNode(Props{"name": "B"})
	_, _ = db1.AddEdge(a, b, "knows", nil)
	db1.Close()

	// Reopen — bloom filter should be rebuilt from disk.
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// The edge should be in the bloom filter.
	if db2.edgeBloom == nil {
		t.Fatal("expected bloom filter to be initialized")
	}
	if !db2.edgeBloom.Test(a, b) {
		t.Error("expected bloom filter to contain edge (a, b) after restart")
	}

	// HasEdge should still work.
	has, err := db2.HasEdge(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("expected HasEdge(a, b) = true after restart")
	}
}

func BenchmarkBloomFilter_Test(b *testing.B) {
	bf := newBloomFilter(1000000)

	// Pre-populate with 100K edges.
	for i := NodeID(1); i <= 100000; i++ {
		bf.Add(i, i+1000000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Test(NodeID(i%100000+1), NodeID(i%100000+1000001))
	}
}

func BenchmarkHasEdge_WithBloom(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create some nodes and edges.
	nodeIDs := make([]NodeID, 100)
	for i := range nodeIDs {
		nodeIDs[i], _ = db.AddNode(Props{"idx": i})
	}
	for i := 0; i < 50; i++ {
		db.AddEdge(nodeIDs[i], nodeIDs[i+50], "test", nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		from := nodeIDs[i%100]
		to := nodeIDs[(i+50)%100]
		db.HasEdge(from, to)
	}
}
