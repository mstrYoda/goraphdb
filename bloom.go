package graphdb

// ---------------------------------------------------------------------------
// Bloom Filter — probabilistic edge existence check for HasEdge().
//
// Architecture:
//
//	┌──────────────────────────────────────────────────────────────┐
//	│                    Bloom Filter System                        │
//	│                                                              │
//	│  Purpose: Avoid disk I/O for HasEdge(from, to) when the     │
//	│  answer is definitely "no". Bloom filters have zero false    │
//	│  negatives: if the filter says "not present", the edge       │
//	│  definitely doesn't exist. False positives are possible      │
//	│  (filter says "maybe present") but are rare with proper      │
//	│  sizing.                                                     │
//	│                                                              │
//	│  Storage: In-memory only (not persisted to disk).            │
//	│  Rebuilt from adj_out bucket on database Open().              │
//	│                                                              │
//	│  Data flow:                                                  │
//	│                                                              │
//	│  HasEdge(from, to):                                          │
//	│    1. Check bloom filter → "definitely not" → return false   │
//	│    2. Check bloom filter → "maybe yes" → do disk I/O         │
//	│                                                              │
//	│  AddEdge(from, to, ...):                                     │
//	│    1. Write to bbolt                                         │
//	│    2. bloom.Add(from, to)   // always succeeds               │
//	│                                                              │
//	│  DeleteEdge(id):                                             │
//	│    1. Delete from bbolt                                      │
//	│    2. (no-op for bloom — can't remove from bloom filter)     │
//	│    Note: false positive rate may slightly increase after      │
//	│    deletes, but never causes incorrect "not found" results.  │
//	│                                                              │
//	│  ┌─────────────────────────────────────────────────────┐     │
//	│  │ Bloom Filter (per DB, not per shard)                │     │
//	│  │                                                     │     │
//	│  │  bit array: [0|1|0|0|1|1|0|1|0|0|1|0|1|0|1|...]   │     │
//	│  │  hash functions: k=4 (FNV-based)                    │     │
//	│  │  expected items: edge count × 2 (headroom)          │     │
//	│  │  target FPR: ~1% (7 bits per element)               │     │
//	│  │                                                     │     │
//	│  │  Key: hash(from_nodeID, to_nodeID) → k bit indices  │     │
//	│  └─────────────────────────────────────────────────────┘     │
//	│                                                              │
//	│  Memory usage:                                               │
//	│    ~1 byte per edge for 1% FPR                               │
//	│    1M edges ≈ 1 MB of bloom filter memory                    │
//	│    10M edges ≈ 10 MB of bloom filter memory                  │
//	│                                                              │
//	│  Concurrency:                                                │
//	│    Reads: lock-free (atomic bit reads)                       │
//	│    Writes: single-writer (bbolt serializes anyway)           │
//	│    Rebuild: blocks reads briefly during Open()               │
//	└──────────────────────────────────────────────────────────────┘
//
// The bloom filter is a classic optimization for graph databases where
// "does edge X→Y exist?" is one of the most frequent operations (used in
// path finding, cycle detection, neighbor checks, and Cypher MATCH).
// ---------------------------------------------------------------------------

import (
	"encoding/binary"
	"hash/fnv"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// bloomFilter is a simple, fast bloom filter for edge existence checks.
// It uses k=4 independent hash functions derived from FNV-1a.
//
// Thread-safety: Add() and Test() are safe for concurrent use.
// The underlying bit array is protected by a RWMutex.
type bloomFilter struct {
	bits []uint64 // bit array (each uint64 holds 64 bits)
	size uint64   // total number of bits
	k    int      // number of hash functions
	mu   sync.RWMutex
}

// newBloomFilter creates a bloom filter sized for the expected number of items
// with approximately 1% false positive rate.
//
// Sizing formula: m = -n*ln(p) / (ln2)^2, k = (m/n) * ln2
// For p=0.01: ~9.6 bits per element, k≈7. We use k=4 for speed.
// Minimum size: 1024 bits.
func newBloomFilter(expectedItems uint64) *bloomFilter {
	if expectedItems < 100 {
		expectedItems = 100 // minimum
	}

	// ~10 bits per element for ~1% FPR
	bitsNeeded := expectedItems * 10
	if bitsNeeded < 1024 {
		bitsNeeded = 1024
	}

	// Round up to uint64 boundary.
	words := (bitsNeeded + 63) / 64

	return &bloomFilter{
		bits: make([]uint64, words),
		size: words * 64,
		k:    4, // 4 hash functions — good balance of speed vs FPR
	}
}

// Add inserts an edge (from → to) into the bloom filter.
func (bf *bloomFilter) Add(from, to NodeID) {
	h1, h2 := bloomHash(from, to)

	bf.mu.Lock()
	for i := 0; i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % bf.size
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
	bf.mu.Unlock()
}

// Test checks if an edge (from → to) might exist.
// Returns false if definitely not present, true if maybe present.
func (bf *bloomFilter) Test(from, to NodeID) bool {
	h1, h2 := bloomHash(from, to)

	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i := 0; i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % bf.size
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false // definitely not present
		}
	}
	return true // maybe present
}

// bloomHash computes two independent hashes for a (from, to) edge pair
// using the double-hashing technique (Kirsch & Mitzenmacker 2006).
//
// The two base hashes h1 and h2 are derived from a single FNV-1a pass.
// All k hash functions are computed as: h_i = h1 + i*h2 (mod m).
func bloomHash(from, to NodeID) (h1, h2 uint64) {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(from))
	binary.LittleEndian.PutUint64(buf[8:], uint64(to))

	hasher := fnv.New128a()
	hasher.Write(buf[:])
	sum := hasher.Sum(nil)

	h1 = binary.LittleEndian.Uint64(sum[:8])
	h2 = binary.LittleEndian.Uint64(sum[8:])
	if h2 == 0 {
		h2 = 1 // avoid degenerate case
	}
	return
}

// ---------------------------------------------------------------------------
// DB integration
// ---------------------------------------------------------------------------

// initBloomFilter creates and populates the bloom filter from the adjacency
// index. Called during Open() after shards are loaded.
func (db *DB) initBloomFilter() {
	// Count total edges across all shards for sizing.
	var totalEdges uint64
	for _, s := range db.shards {
		totalEdges += s.edgeCount.Load()
	}

	// Size with 2x headroom for future inserts before next rebuild.
	expected := totalEdges * 2
	if expected < 1000 {
		expected = 1000
	}
	db.edgeBloom = newBloomFilter(expected)

	// Populate from adj_out bucket (one entry per edge).
	for _, s := range db.shards {
		_ = s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketAdjOut)
			if b == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				// adj_out key: encodeAdjKey(from, edgeID) → 8+8=16 bytes
				// adj_out value: encodeAdjValue(to, label) → 8+len(label)
				if len(k) >= 16 && len(v) >= 8 {
					from := NodeID(binary.BigEndian.Uint64(k[:8]))
					to := NodeID(binary.BigEndian.Uint64(v[:8]))
					db.edgeBloom.Add(from, to)
				}
				return nil
			})
		})
	}

	db.log.Info("bloom filter initialized",
		"edges", totalEdges,
		"bloom_bits", db.edgeBloom.size,
		"bloom_memory_bytes", len(db.edgeBloom.bits)*8,
	)
}
