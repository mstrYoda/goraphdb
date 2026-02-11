package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Shared node registry — writers publish created node IDs so readers and edge
// creators can target real nodes.
// ---------------------------------------------------------------------------

type NodeRegistry struct {
	mu  sync.RWMutex
	ids []uint64
}

func (r *NodeRegistry) Add(id uint64) {
	r.mu.Lock()
	r.ids = append(r.ids, id)
	r.mu.Unlock()
}

func (r *NodeRegistry) Len() int {
	r.mu.RLock()
	n := len(r.ids)
	r.mu.RUnlock()
	return n
}

// Random returns a random node ID, or 0 if the registry is empty.
func (r *NodeRegistry) Random(rng *rand.Rand) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ids) == 0 {
		return 0
	}
	return r.ids[rng.Intn(len(r.ids))]
}

// RandomPair returns two different random node IDs.
func (r *NodeRegistry) RandomPair(rng *rand.Rand) (uint64, uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := len(r.ids)
	if n < 2 {
		return 0, 0
	}
	i := rng.Intn(n)
	j := rng.Intn(n - 1)
	if j >= i {
		j++
	}
	return r.ids[i], r.ids[j]
}

// ---------------------------------------------------------------------------
// HTTP helpers.
// ---------------------------------------------------------------------------

var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	},
}

func postJSON(url string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return httpClient.Post(url, "application/json", bytes.NewReader(data))
}

func readAndClose(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// ---------------------------------------------------------------------------
// Target rotator — round-robin across cluster nodes.
// ---------------------------------------------------------------------------

type TargetRotator struct {
	targets []string
	idx     atomic.Uint64
}

func NewTargetRotator(targets []string) *TargetRotator {
	return &TargetRotator{targets: targets}
}

func (t *TargetRotator) Next() string {
	i := t.idx.Add(1) - 1
	return t.targets[i%uint64(len(t.targets))]
}

// ---------------------------------------------------------------------------
// Write worker — creates nodes, edges, merges, updates.
// ---------------------------------------------------------------------------

func runWriteWorker(
	ctx context.Context,
	id int,
	targets *TargetRotator,
	registry *NodeRegistry,
	collector *Collector,
	warmup bool,
) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	edgeLabels := []string{"FOLLOWS", "KNOWS", "LIKES", "WORKS_WITH", "RELATED_TO"}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Decide what operation to run based on the state of the registry.
		// We always create nodes first to build up the registry, then mix in
		// edges, merges, and updates once we have enough nodes.
		nReg := registry.Len()
		roll := rng.Intn(100)

		switch {
		case nReg < 10 || roll < 40:
			// 40% — CREATE NODE
			doCreateNode(ctx, targets.Next(), rng, registry, collector, warmup)

		case roll < 70:
			// 30% — CREATE EDGE
			doCreateEdge(ctx, targets.Next(), rng, registry, collector, edgeLabels, warmup)

		case roll < 85:
			// 15% — MERGE NODE (Cypher MERGE)
			doMergeNode(ctx, targets.Next(), rng, registry, collector, warmup)

		default:
			// 15% — UPDATE NODE
			doUpdateNode(ctx, targets.Next(), rng, registry, collector, warmup)
		}
	}
}

// doCreateNode creates a node via the REST API and registers its ID.
func doCreateNode(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	registry *NodeRegistry,
	collector *Collector,
	warmup bool,
) {
	name := fmt.Sprintf("user_%d", rng.Intn(1_000_000))
	age := rng.Intn(80) + 18
	city := cities[rng.Intn(len(cities))]

	start := time.Now()
	resp, err := postJSON(target+"/api/nodes", map[string]any{
		"props": map[string]any{
			"name":   name,
			"age":    age,
			"city":   city,
			"_bench": true,
		},
	})
	dur := time.Since(start)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)

	if !warmup {
		collector.Record(Sample{Op: OpCreateNode, Duration: dur, IsError: isErr})
	}

	if !isErr && resp != nil {
		var result struct {
			ID uint64 `json:"id"`
		}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if result.ID > 0 {
			registry.Add(result.ID)
		}
	} else {
		readAndClose(resp)
	}
}

// doCreateEdge creates an edge between two random existing nodes.
func doCreateEdge(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	registry *NodeRegistry,
	collector *Collector,
	labels []string,
	warmup bool,
) {
	from, to := registry.RandomPair(rng)
	if from == 0 || to == 0 {
		return
	}

	label := labels[rng.Intn(len(labels))]
	body := map[string]any{
		"from":  from,
		"to":    to,
		"label": label,
		"props": map[string]any{"weight": rng.Float64()},
	}

	start := time.Now()
	resp, err := postJSON(target+"/api/edges", body)
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	if !warmup {
		collector.Record(Sample{Op: OpCreateEdge, Duration: dur, IsError: isErr})
	}
}

// doMergeNode uses Cypher MERGE to upsert a node.
func doMergeNode(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	registry *NodeRegistry,
	collector *Collector,
	warmup bool,
) {
	// MERGE on a name that may or may not exist.
	name := fmt.Sprintf("merge_user_%d", rng.Intn(500)) // small range to cause matches
	query := fmt.Sprintf(
		`MERGE (n:LoadTest {name: "%s"}) ON CREATE SET n.created = "%s" ON MATCH SET n.updated = "%s" RETURN n`,
		name, time.Now().Format(time.RFC3339), time.Now().Format(time.RFC3339),
	)

	start := time.Now()
	resp, err := postJSON(target+"/api/cypher", map[string]string{"query": query})
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	if !warmup {
		collector.Record(Sample{Op: OpMergeNode, Duration: dur, IsError: isErr})
	}
}

// doUpdateNode updates a random existing node via the REST API.
//
// Uses PUT /api/nodes/{id} which is a direct UpdateNode call — much faster
// than the Cypher MATCH...SET path since there's no query parsing or
// pattern matching overhead.
func doUpdateNode(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	registry *NodeRegistry,
	collector *Collector,
	warmup bool,
) {
	id := registry.Random(rng)
	if id == 0 {
		return
	}

	url := fmt.Sprintf("%s/api/nodes/%d", target, id)
	body := map[string]any{
		"props": map[string]any{
			"updated": time.Now().Format(time.RFC3339),
			"score":   rng.Float64(),
		},
	}

	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := httpClient.Do(req)
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	if !warmup {
		collector.Record(Sample{Op: OpUpdateNode, Duration: dur, IsError: isErr})
	}
}

// ---------------------------------------------------------------------------
// Read worker — point reads, label scans, traversals, neighborhoods.
// ---------------------------------------------------------------------------

func runReadWorker(
	ctx context.Context,
	id int,
	targets *TargetRotator,
	registry *NodeRegistry,
	collector *Collector,
) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*31))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Wait for some nodes to exist before reading.
		if registry.Len() < 2 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		roll := rng.Intn(100)
		target := targets.Next()

		switch {
		case roll < 25:
			// 25% — Point read by ID (REST)
			doPointRead(ctx, target, rng, registry, collector)
		case roll < 45:
			// 20% — Indexed property lookup (Cypher + index)
			doIndexedLookup(ctx, target, rng, collector)
		case roll < 60:
			// 15% — Label scan (Cypher)
			doLabelScan(ctx, target, collector)
		case roll < 80:
			// 20% — 1-hop traversal (REST neighborhood)
			doTraversal(ctx, target, rng, registry, collector)
		default:
			// 20% — Full neighborhood fetch (REST)
			doNeighborhood(ctx, target, rng, registry, collector)
		}
	}
}

// doPointRead reads a single node by ID.
func doPointRead(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	registry *NodeRegistry,
	collector *Collector,
) {
	id := registry.Random(rng)
	if id == 0 {
		return
	}

	url := fmt.Sprintf("%s/api/nodes/%d", target, id)
	start := time.Now()
	resp, err := httpClient.Get(url)
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	collector.Record(Sample{Op: OpPointRead, Duration: dur, IsError: isErr})
}

// doLabelScan runs a Cypher label scan.
func doLabelScan(
	ctx context.Context,
	target string,
	collector *Collector,
) {
	query := `MATCH (n:LoadTest) RETURN n.name LIMIT 100`

	start := time.Now()
	resp, err := postJSON(target+"/api/cypher", map[string]string{"query": query})
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	collector.Record(Sample{Op: OpLabelScan, Duration: dur, IsError: isErr})
}

// doTraversal runs a 1-hop traversal from a random node using the REST API.
//
// NOTE: We use GET /api/nodes/{id}/neighborhood instead of Cypher
// `MATCH (a)-[r]->(b) WHERE id(a) = N` because the Cypher engine does NOT
// have an optimization path for `WHERE id(a) = N` in single-hop patterns.
// Without the optimization, it does a full edge scan which is O(edges) and
// triggers slow query warnings as the dataset grows.
// The REST neighborhood endpoint does a direct O(1) node lookup + edge fetch.
func doTraversal(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	registry *NodeRegistry,
	collector *Collector,
) {
	id := registry.Random(rng)
	if id == 0 {
		return
	}

	url := fmt.Sprintf("%s/api/nodes/%d/neighborhood", target, id)
	start := time.Now()
	resp, err := httpClient.Get(url)
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	collector.Record(Sample{Op: OpTraversal, Duration: dur, IsError: isErr})
}

// doNeighborhood fetches the neighborhood of a random node.
func doNeighborhood(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	registry *NodeRegistry,
	collector *Collector,
) {
	id := registry.Random(rng)
	if id == 0 {
		return
	}

	url := fmt.Sprintf("%s/api/nodes/%d/neighborhood", target, id)
	start := time.Now()
	resp, err := httpClient.Get(url)
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	collector.Record(Sample{Op: OpNeighborhood, Duration: dur, IsError: isErr})
}

// ---------------------------------------------------------------------------
// Indexed property lookup — uses Cypher with indexed property equality.
// ---------------------------------------------------------------------------

// doIndexedLookup queries nodes by an indexed property (name or city).
func doIndexedLookup(
	ctx context.Context,
	target string,
	rng *rand.Rand,
	collector *Collector,
) {
	var query string
	if rng.Intn(2) == 0 {
		// Lookup by name (exact match via property index).
		query = fmt.Sprintf(`MATCH (n {name: "seed_%d"}) RETURN n`, rng.Intn(200))
	} else {
		// Lookup by city (exact match via property index).
		city := cities[rng.Intn(len(cities))]
		query = fmt.Sprintf(`MATCH (n {city: "%s"}) RETURN n LIMIT 20`, city)
	}

	start := time.Now()
	resp, err := postJSON(target+"/api/cypher", map[string]string{"query": query})
	dur := time.Since(start)
	readAndClose(resp)

	isErr := err != nil || (resp != nil && resp.StatusCode >= 400)
	collector.Record(Sample{Op: OpIndexedLookup, Duration: dur, IsError: isErr})
}

// ---------------------------------------------------------------------------
// Index setup — creates secondary indexes on commonly queried properties.
// ---------------------------------------------------------------------------

func createIndexes(target string) error {
	indexes := []string{"name", "city"}
	for _, prop := range indexes {
		resp, err := postJSON(target+"/api/indexes", map[string]string{
			"property": prop,
		})
		if err != nil {
			return fmt.Errorf("create index %q: %w", prop, err)
		}
		// 409 Conflict means index already exists — that's fine.
		if resp.StatusCode >= 400 && resp.StatusCode != 409 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("create index %q: HTTP %d: %s", prop, resp.StatusCode, string(body))
		}
		readAndClose(resp)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Seed data — generate enough nodes during warmup for meaningful reads.
// ---------------------------------------------------------------------------

func seedNodes(ctx context.Context, target string, count int, registry *NodeRegistry) error {
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := postJSON(target+"/api/nodes", map[string]any{
			"props": map[string]any{
				"name":   fmt.Sprintf("seed_%d", i),
				"age":    18 + (i % 60),
				"city":   cities[i%len(cities)],
				"_bench": true,
			},
		})
		if err != nil {
			return fmt.Errorf("seed node %d: %w", i, err)
		}
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("seed node %d: HTTP %d: %s", i, resp.StatusCode, string(body))
		}

		var result struct {
			ID uint64 `json:"id"`
		}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if result.ID > 0 {
			registry.Add(result.ID)
		}
	}
	return nil
}

func seedEdges(ctx context.Context, target string, count int, registry *NodeRegistry) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	labels := []string{"FOLLOWS", "KNOWS", "LIKES", "WORKS_WITH"}

	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		from, to := registry.RandomPair(rng)
		if from == 0 || to == 0 {
			continue
		}

		resp, err := postJSON(target+"/api/edges", map[string]any{
			"from":  from,
			"to":    to,
			"label": labels[rng.Intn(len(labels))],
			"props": map[string]any{"weight": rng.Float64()},
		})
		if err != nil {
			return fmt.Errorf("seed edge %d: %w", i, err)
		}
		readAndClose(resp)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Reference data for random property generation.
// ---------------------------------------------------------------------------

var cities = []string{
	"Istanbul", "Ankara", "Izmir", "Berlin", "London",
	"Paris", "Tokyo", "New York", "San Francisco", "Mumbai",
	"Sydney", "Toronto", "Seoul", "Amsterdam", "Dubai",
	"Singapore", "Bangkok", "Barcelona", "Rome", "Vienna",
}
