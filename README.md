# GraphDB

A high-performance, embeddable graph database written in Go. Built on top of [bbolt](https://github.com/etcd-io/bbolt) (B+tree key-value store), it supports concurrent queries, secondary indexes, optional hash-based sharding, and a subset of the Cypher query language — all in a single dependency-free binary.

![Query Editor Screenshot](assets/query.png)

## Features

- **Directed labeled graph** — nodes and edges with arbitrary JSON-like properties  
  `alice ---follows---> bob`, `server ---consumes---> queue`
- **Node labels** — first-class `:Person`, `:Movie` labels with dedicated index and Cypher support (`MATCH (n:Person)`)
- **Concurrent reads** — fully parallel BFS, DFS, Cypher, and query-builder calls via MVCC
- **50 GB+ ready** — bbolt memory-mapped storage with configurable `MmapSize`
- **Graph algorithms** — BFS, DFS, Shortest Path (unweighted & Dijkstra), All Paths, Connected Components, Topological Sort
- **Fluent query builder** — chainable Go API with filtering, pagination, and direction control
- **Secondary indexes** — O(log N) property lookups with auto-maintenance on single writes
- **Composite indexes** — multi-property indexes for fast compound lookups (`CreateCompositeIndex("city", "age")`)
- **Cypher query language** — read and write support with index-aware execution, LIMIT push-down, ORDER BY + LIMIT heap, query plan caching, `OPTIONAL MATCH`, `EXPLAIN`/`PROFILE`, parameterized queries, and `CREATE` for inserting nodes and edges
- **Query timeout** — `CypherContext`/`CypherWithParamsContext` accept `context.Context` for deadline-based cancellation at scan loop boundaries
- **Transactions** — `Begin`/`Commit`/`Rollback` API for multi-statement atomic operations with read-your-writes semantics
- **EXPLAIN / PROFILE** — query plan tree with operator types; `PROFILE` adds per-operator row counts and wall-clock timing
- **OPTIONAL MATCH** — left-outer-join semantics for graph patterns (unmatched bindings become `nil`)
- **Byte-budgeted node cache** — sharded concurrent LRU cache with memory-based eviction (default 128 MB); predictable memory footprint regardless of node sizes
- **Data integrity** — CRC32 (Castagnoli) checksums on all node/edge data, verified on every read, with a `VerifyIntegrity()` full scan
- **Binary encoding** — MessagePack property serialization (3–5× faster, 30–50% smaller than JSON) with backward-compatible format detection
- **Structured logging** — `log/slog` integration for all write operations, errors, and lifecycle events
- **Parameterized queries** — `$param` tokens in Cypher for safe substitution and plan reuse
- **Prepared statement caching** — bounded LRU query cache (10K entries) with `PrepareCypher`/`ExecutePrepared`/`ExecutePreparedWithParams` API and server-side `/api/cypher/prepare` + `/api/cypher/execute` endpoints
- **Streaming results** — `CypherStream()` returns a lazy `RowIterator` for O(1) memory on non-sorted queries; NDJSON streaming via `POST /api/cypher/stream`
- **Slow query log** — configurable threshold (default 100ms); queries exceeding the threshold are logged at WARN level with duration, row count, and truncated query text
- **Cursor pagination** — O(limit) cursor-based `ListNodes`/`ListEdges`/`ListNodesByLabel` APIs; no offset scanning. Server endpoints: `GET /api/nodes/cursor`, `GET /api/edges/cursor`
- **Prometheus metrics** — dependency-free atomic counters with Prometheus text exposition at `GET /metrics`; tracks queries, slow queries, cache hits/misses, node/edge CRUD, index lookups, and live gauges
- **Batch operations** — `AddNodeBatch` / `AddEdgeBatch` for bulk loading with single-fsync transactions
- **Worker pool** — built-in goroutine pool for concurrent query execution
- **Optional sharding** — hash-based partitioning across multiple bbolt files; edges co-located with source nodes for single-shard traversals
- **Management UI** — built-in web console with a Cypher query editor, interactive graph visualization (cytoscape.js), index management, and a node/edge explorer

## Installation

```bash
go get github.com/mstrYoda/goraphdb
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
    // Open (or create) a database.
    db, err := graphdb.Open("./my.db", graphdb.DefaultOptions())
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Add nodes with arbitrary properties.
    alice, _ := db.AddNode(graphdb.Props{"name": "Alice", "age": 30})
    bob, _ := db.AddNode(graphdb.Props{"name": "Bob", "age": 25})

    // Add a directed labeled edge.
    db.AddEdge(alice, bob, "follows", graphdb.Props{"since": "2024"})

    // Query neighbors.
    neighbors, _ := db.NeighborsLabeled(alice, "follows")
    for _, n := range neighbors {
        fmt.Println(n.GetString("name")) // Bob
    }

    // BFS traversal.
    results, _ := db.BFSCollect(alice, 3, graphdb.Outgoing)
    for _, r := range results {
        fmt.Printf("depth=%d  %s\n", r.Depth, r.Node.GetString("name"))
    }

    // Cypher query.
    ctx := context.Background()
    res, _ := db.Cypher(ctx, `MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)
    for _, row := range res.Rows {
        fmt.Println(row["b.name"]) // Bob
    }
}
```

## Configuration

```go
opts := graphdb.Options{
    ShardCount:         1,                      // 1 = single process (default), N = hash-sharded
    WorkerPoolSize:     8,                      // goroutines for concurrent query execution
    CacheBudget:        128 * 1024 * 1024,      // 128 MB byte-budget LRU cache for hot nodes
    SlowQueryThreshold: 100 * time.Millisecond, // log queries slower than this (0 = disabled)
    NoSync:             false,                  // true = skip fsync (faster writes, risk of data loss)
    ReadOnly:           false,                  // open in read-only mode
    MmapSize:           256 * 1024 * 1024,      // 256 MB initial mmap
}
db, err := graphdb.Open("./data", opts)
```

Use `graphdb.DefaultOptions()` for sensible defaults tuned for ~50 GB datasets.

## API Reference

### Node Operations

```go
// Create
id, err := db.AddNode(graphdb.Props{"name": "Alice", "age": 30})
ids, err := db.AddNodeBatch([]graphdb.Props{...})    // bulk insert (single tx)

// Read
node, err := db.GetNode(id)
name := node.GetString("name")      // "Alice"
age  := node.GetFloat("age")        // 30.0
exists, err := db.NodeExists(id)
count := db.NodeCount()

// Update
err = db.UpdateNode(id, graphdb.Props{"age": 31})    // merge
err = db.SetNodeProps(id, graphdb.Props{"name": "A"}) // full replace

// Delete
err = db.DeleteNode(id)              // also removes all connected edges

// Scan & Filter
nodes, err := db.FindNodes(func(n *graphdb.Node) bool {
    return n.GetFloat("age") > 25
})
err = db.ForEachNode(func(n *graphdb.Node) error {
    fmt.Println(n.Props)
    return nil
})
```

### Node Labels

```go
// Create a node with labels
id, err := db.AddNodeWithLabels([]string{"Person", "Employee"}, graphdb.Props{"name": "Alice"})

// Add / remove labels on existing nodes
err = db.AddLabel(id, "Admin")
err = db.RemoveLabel(id, "Employee")

// Query labels
labels, err := db.GetLabels(id)           // ["Person", "Admin"]
has, err := db.HasLabel(id, "Person")     // true

// Find all nodes with a label (index-backed)
people, err := db.FindByLabel("Person")
```

### Transactions

```go
// Multi-statement atomic operations with read-your-writes semantics.
tx, err := db.Begin()

alice, _ := tx.AddNode(graphdb.Props{"name": "Alice"})
bob, _ := tx.AddNode(graphdb.Props{"name": "Bob"})
tx.AddEdge(alice, bob, "follows", nil)

// Read uncommitted data within the same transaction.
node, _ := tx.GetNode(alice) // visible before commit

err = tx.Commit()   // atomically persists all changes
// — or —
err = tx.Rollback() // discards all changes
```

### Edge Operations

```go
// Create  —  alice ---follows---> bob
edgeID, err := db.AddEdge(alice, bob, "follows", graphdb.Props{"since": "2024"})
ids, err := db.AddEdgeBatch([]graphdb.Edge{...})

// Read
edge, err := db.GetEdge(edgeID)
outEdges, err := db.OutEdges(alice)                   // all outgoing
inEdges, err := db.InEdges(bob)                       // all incoming
allEdges, err := db.Edges(alice)                      // both directions
labeled, err := db.OutEdgesLabeled(alice, "follows")  // by label
byLabel, err := db.EdgesByLabel("follows")            // all edges with label
count := db.EdgeCount()

// Update
err = db.UpdateEdge(edgeID, graphdb.Props{"weight": 1.5})

// Delete
err = db.DeleteEdge(edgeID)

// Predicates
has, err := db.HasEdge(alice, bob)
has, err := db.HasEdgeLabeled(alice, bob, "follows")
deg, err := db.Degree(alice, graphdb.Outgoing)

// Neighbors
nodes, err := db.Neighbors(alice)                          // outgoing neighbors
nodes, err := db.NeighborsLabeled(alice, "follows")        // filtered by label
nodes, err := db.NeighborsDirection(alice, graphdb.Both)   // both directions
```

### Traversal Algorithms

```go
// BFS — breadth-first search with visitor callback
err = db.BFS(startID, maxDepth, graphdb.Outgoing, edgeFilter, func(r *graphdb.TraversalResult) bool {
    fmt.Printf("depth=%d node=%v\n", r.Depth, r.Node.Props["name"])
    return true // return false to stop early
})

// Convenience collectors
results, err := db.BFSCollect(startID, 3, graphdb.Outgoing)
results, err := db.DFSCollect(startID, 3, graphdb.Outgoing)

// Filtered traversals
results, err := db.BFSFiltered(startID, 3, graphdb.Outgoing, edgeFilter, nodeFilter)
results, err := db.DFSFiltered(startID, 3, graphdb.Outgoing, edgeFilter, nodeFilter)
```

### Pathfinding

```go
// Shortest path (unweighted BFS)
path, err := db.ShortestPath(from, to)
path, err := db.ShortestPathLabeled(from, to, "follows")

// Dijkstra (weighted)
path, err := db.ShortestPathWeighted(from, to, "weight", 1.0)

// All paths (up to maxDepth)
paths, err := db.AllPaths(from, to, 5)

// Connectivity
exists, err := db.HasPath(from, to)
components, err := db.ConnectedComponents()
sorted, err := db.TopologicalSort()  // Kahn's algorithm, errors on cycles
```

### Secondary Indexes

```go
// Create an index on a property (scans existing nodes)
err = db.CreateIndex("name")

// Fast lookup — O(log N) via B+tree prefix scan
nodes, err := db.FindByProperty("name", "Alice")

// Check if a property is indexed
indexed := db.HasIndex("name")

// Drop / rebuild
err = db.DropIndex("name")
err = db.ReIndex("name")
```

> **Index maintenance**: `AddNode`, `UpdateNode`, `SetNodeProps`, and `DeleteNode` automatically update indexes within the same transaction (zero extra fsync). `AddNodeBatch` skips auto-indexing for performance — call `CreateIndex()` or `ReIndex()` after batch inserts.

### Composite Indexes

```go
// Create a composite index on multiple properties (scans existing nodes)
err = db.CreateCompositeIndex("city", "age")

// Fast compound lookup — O(log N) via B+tree prefix scan
nodes, err := db.FindByCompositeIndex(map[string]any{"city": "Istanbul", "age": 30})

// Cypher queries use composite indexes automatically
// MATCH (n {city: "Istanbul", age: 30}) RETURN n  → composite index seek

// Management
has := db.HasCompositeIndex("city", "age")
indexes := db.ListCompositeIndexes() // [][]string
err = db.DropCompositeIndex("city", "age")
```

### Prepared Statements & Query Cache

```go
// Prepare a parameterized query (parsed once, cached)
pq, err := db.PrepareCypher("MATCH (n {name: $name}) RETURN n")

ctx := context.Background()

// Execute with different parameters — no re-parsing
result, err := db.ExecutePreparedWithParams(ctx, pq, map[string]any{"name": "Alice"})
result, err = db.ExecutePreparedWithParams(ctx, pq, map[string]any{"name": "Bob"})

// Execute without parameters
result, err = db.ExecutePrepared(ctx, pq)

// Query cache statistics (bounded LRU, default 10K entries)
stats := db.QueryCacheStats()
fmt.Printf("hits=%d misses=%d entries=%d\n", stats.Hits, stats.Misses, stats.Entries)
```

### Streaming Results (Iterator)

```go
ctx := context.Background()

// CypherStream returns a lazy RowIterator — O(1) memory for non-sorted queries.
iter, err := db.CypherStream(ctx, "MATCH (n) RETURN n.name LIMIT 100")
if err != nil {
    log.Fatal(err)
}
defer iter.Close()

for iter.Next() {
    row := iter.Row()
    fmt.Println(row["n.name"])
}
if err := iter.Err(); err != nil {
    log.Fatal(err)
}

// Parameterized streaming
iter, err = db.CypherStreamWithParams(ctx,
    "MATCH (n {city: $city}) RETURN n.name",
    map[string]any{"city": "Istanbul"},
)
```

### Write Cypher (CREATE)

```go
ctx := context.Background()

// CREATE works through the unified Cypher() API — no separate function needed.
result, err := db.Cypher(ctx, `CREATE (n:Person {name: "Alice", age: 30}) RETURN n`)
node := result.Rows[0]["n"].(*graphdb.Node) // access created node

// Create two nodes and an edge in one statement.
db.Cypher(ctx, `CREATE (a:Person {name: "Alice"})-[:FOLLOWS]->(b:Person {name: "Bob"})`)

// Multiple comma-separated patterns.
db.Cypher(ctx, `CREATE (a:City {name: "Istanbul"}), (b:City {name: "Ankara"})`)

// CREATE without RETURN — fire-and-forget.
db.Cypher(ctx, `CREATE (n:Movie {title: "The Matrix", year: 1999})`)

// Dedicated API with creation statistics.
cr, _ := db.CypherCreate(ctx, `CREATE (n:Person {name: "Eve"}) RETURN n`)
fmt.Println(cr.Stats.NodesCreated) // 1
fmt.Println(cr.Stats.LabelsSet)    // 1
fmt.Println(cr.Stats.PropsSet)     // 1
```

### Query Timeout & Cancellation

```go
// All query methods accept context.Context as the first argument for
// timeout/cancellation. The context is checked at key iteration points
// (full scans, edge traversals, index scans).

ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
defer cancel()

result, err := db.Cypher(ctx, `MATCH (n) RETURN n`)
if errors.Is(err, context.DeadlineExceeded) {
    log.Println("query timed out")
}

// Parameterized queries also accept context:
result, err = db.CypherWithParams(ctx,
    "MATCH (n {name: $name}) RETURN n",
    map[string]any{"name": "Alice"},
)
```

### Cursor Pagination

```go
// List nodes with cursor-based pagination — O(limit) per page, no offset scan.
page, err := db.ListNodes(0, 20) // first page, 20 nodes
for _, n := range page.Nodes {
    fmt.Printf("id=%d name=%s\n", n.ID, n.GetString("name"))
}

// Next page: pass the cursor from the previous page.
if page.HasMore {
    page2, _ := db.ListNodes(page.NextCursor, 20)
    // ...
}

// Edges and label-filtered nodes also supported.
edgePage, _ := db.ListEdges(0, 50)
labelPage, _ := db.ListNodesByLabel("Person", 0, 20)
```

### Slow Query Log

```go
// Queries exceeding SlowQueryThreshold are logged at WARN level automatically.
opts := graphdb.DefaultOptions()
opts.SlowQueryThreshold = 50 * time.Millisecond // default: 100ms, 0 = disabled

// Log output (slog):
// WARN slow query detected query="MATCH (n) RETURN n" duration=152ms rows=50000
```

### Prometheus Metrics

```go
// All metrics are atomic counters — zero contention, no external dependencies.
m := db.Metrics()

// Programmatic access
snap := m.Snapshot() // map[string]any with all counters + live gauges

// Prometheus text exposition (for /metrics endpoint or manual use)
m.WritePrometheus(os.Stdout)
// Output:
// # HELP graphdb_queries_total Total number of Cypher query executions
// # TYPE graphdb_queries_total counter
// graphdb_queries_total 42
// ...
```

Available metrics:
- **Counters**: `graphdb_queries_total`, `graphdb_slow_queries_total`, `graphdb_query_errors_total`, `graphdb_cache_hits_total`, `graphdb_cache_misses_total`, `graphdb_nodes_created_total`, `graphdb_nodes_deleted_total`, `graphdb_edges_created_total`, `graphdb_edges_deleted_total`, `graphdb_index_lookups_total`
- **Gauges**: `graphdb_nodes_current`, `graphdb_edges_current`, `graphdb_node_cache_bytes_used`, `graphdb_node_cache_budget_bytes`, `graphdb_query_cache_entries`, `graphdb_query_cache_capacity`

### Fluent Query Builder

```go
result, err := db.NewQuery().
    From(alice).
    FollowEdge("follows").
    Dir(graphdb.Outgoing).
    Depth(3).
    Where(func(n *graphdb.Node) bool {
        return n.GetFloat("age") > 25
    }).
    Limit(10).
    Offset(0).
    UseBFS().        // or .UseDFS()
    Execute()

for _, node := range result.Nodes {
    fmt.Println(node.GetString("name"))
}
```

### Concurrent Queries

```go
ctx := context.Background()

// Run multiple queries in parallel using the built-in worker pool.
results, err := db.NewConcurrentQuery().
    Add(db.NewQuery().From(alice).FollowEdge("follows").Depth(2)).
    Add(db.NewQuery().From(bob).FollowEdge("follows").Depth(2)).
    Execute(ctx)

// Or run arbitrary functions concurrently.
values, errs := db.ExecuteFunc(ctx,
    func() (interface{}, error) { return db.ShortestPath(alice, charlie) },
    func() (interface{}, error) { return db.BFSCollect(bob, 3, graphdb.Outgoing) },
)
```

### Cypher Query Language

GraphDB supports a read-only subset of the [Cypher](https://neo4j.com/docs/cypher-manual/current/) query language with index-aware execution, LIMIT push-down, and query plan caching.

#### Supported Patterns

```go
ctx := context.Background()

// 1. All nodes
res, _ := db.Cypher(ctx, `MATCH (n) RETURN n`)

// 2. Property filter (uses index if available)
res, _ = db.Cypher(ctx, `MATCH (n {name: "Alice"}) RETURN n`)

// 3. WHERE clause
res, _ = db.Cypher(ctx, `MATCH (n) WHERE n.age > 25 RETURN n`)

// 4. Single-hop pattern match
res, _ = db.Cypher(ctx, `MATCH (a)-[:follows]->(b) RETURN a, b`)

// 5. Filtered traversal with property projection
res, _ = db.Cypher(ctx, `MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)

// 6. Variable-length path (1 to 3 hops)
res, _ = db.Cypher(ctx, `MATCH (a)-[:follows*1..3]->(b) RETURN b`)

// 7. Any edge type with type() function
res, _ = db.Cypher(ctx, `MATCH (a)-[r]->(b) RETURN type(r), b`)

// 8. Label-based matching (index-backed)
res, _ = db.Cypher(ctx, `MATCH (n:Person) RETURN n`)
res, _ = db.Cypher(ctx, `MATCH (a:Person)-[:follows]->(b:Person) RETURN a, b`)

// 9. OPTIONAL MATCH — left-outer-join (nil when no match)
res, _ = db.Cypher(ctx, `MATCH (n:Person) OPTIONAL MATCH (n)-[r:WROTE]->(b) RETURN n.name, b`)
```

#### EXPLAIN / PROFILE

```go
// EXPLAIN — returns the query plan without executing (zero I/O)
res, _ := db.Cypher(ctx, `EXPLAIN MATCH (n:Person) WHERE n.age > 25 RETURN n`)
fmt.Println(res.Plan.String())
// EXPLAIN:
// └── ProduceResults (n)
//     └── Filter (WHERE clause)
//         └── NodeByLabelScan (n:Person)

// PROFILE — executes and returns the plan annotated with actual row counts + timing
res, _ = db.Cypher(ctx, `PROFILE MATCH (n:Person) RETURN n`)
fmt.Println(res.Plan.String())
// PROFILE:
// └── ProduceResults (n) [rows=42, time=150µs]
//     └── NodeByLabelScan (n:Person) [rows=42]

// The actual query results are still available:
for _, row := range res.Rows {
    fmt.Println(row["n"])
}
```

#### Parameterized Queries

```go
// Use $param tokens to prevent injection and enable plan caching.
res, _ := db.CypherWithParams(ctx,
    `MATCH (n {name: $name}) WHERE n.age > $minAge RETURN n`,
    map[string]any{"name": "Alice", "minAge": 25},
)
```

#### ORDER BY, LIMIT, Prepared Queries

```go
ctx := context.Background()

// ORDER BY + LIMIT — uses a min-heap for top-K efficiency
res, _ := db.Cypher(ctx, `MATCH (n) WHERE n.age > 20 RETURN n.name ORDER BY n.age DESC LIMIT 5`)

// LIMIT push-down — stops scanning early when no ORDER BY is present
res, _ = db.Cypher(ctx, `MATCH (n) RETURN n LIMIT 10`)

// Prepared queries — parse once, execute many times
pq, _ := db.PrepareCypher(`MATCH (n {name: "Alice"})-[:follows]->(b) RETURN b.name`)
res1, _ := db.ExecutePrepared(ctx, pq)
res2, _ := db.ExecutePrepared(ctx, pq) // no re-parsing

// Results
for _, row := range res.Rows {
    fmt.Println(row["n.name"], row["n.age"])
}
```

### Statistics

```go
stats, err := db.Stats()
fmt.Printf("Nodes: %d, Edges: %d, Shards: %d, Disk: %.1f MB\n",
    stats.NodeCount, stats.EdgeCount, stats.ShardCount,
    float64(stats.DiskSizeBytes)/1024/1024)
```

### Data Integrity

```go
// Verify all node and edge data across all shards (CRC32 checksums).
report, err := db.VerifyIntegrity()
fmt.Printf("Checked %d nodes, %d edges\n", report.NodesChecked, report.EdgesChecked)

if report.OK() {
    fmt.Println("All data intact!")
} else {
    for _, e := range report.Errors {
        fmt.Println(e) // "shard 0, nodes[00000001]: props checksum mismatch ..."
    }
}
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Management UI                            │
│   React + TypeScript + Tailwind · cytoscape.js · CodeMirror  │
│   Query Editor · Dashboard · Indexes · Explorer              │
├──────────────────────────────────────────────────────────────┤
│                    HTTP / JSON API                            │
│   /api/cypher · /api/nodes · /api/edges · /api/indexes       │
│   /api/stats · CORS · SPA fallback                           │
├──────────────────────────────────────────────────────────────┤
│                        Public API                            │
│   Node/Edge CRUD · Labels · Transactions (Begin/Commit)      │
│   BFS/DFS · Paths · Query Builder · VerifyIntegrity          │
├──────────────────────────────────────────────────────────────┤
│                     Cypher Engine                            │
│   Lexer → Parser → AST → Executor (index-aware)             │
│   EXPLAIN/PROFILE · OPTIONAL MATCH · Parameterized ($param)  │
│   Query plan cache · LIMIT push-down · Top-K heap           │
├──────────────────────────────────────────────────────────────┤
│                    Shard Manager                             │
│   Hash-based routing · Cross-shard edge handling             │
│   Worker pool · Sharded LRU node cache                       │
├──────────────────────────────────────────────────────────────┤
│                   Storage Layer                              │
│   bbolt (B+tree) · Memory-mapped files · MVCC               │
│   MessagePack encoding · CRC32 checksums · Labels index      │
├──────────────────────────────────────────────────────────────┤
│  nodes│edges│adj_*│idx_prop│idx_edge_type│node_labels│idx_lbl│
└──────────────────────────────────────────────────────────────┘
```

### Storage Layout (bbolt buckets)

| Bucket | Key | Value | Purpose |
|---|---|---|---|
| `nodes` | `uint64 nodeID` | MessagePack props + CRC32 | Node data |
| `edges` | `uint64 edgeID` | Binary edge + CRC32 | Edge data (from, to, label, props) |
| `adj_out` | `nodeID \| edgeID` | `targetID \| label` | Outgoing adjacency list |
| `adj_in` | `nodeID \| edgeID` | `sourceID \| label` | Incoming adjacency list |
| `idx_prop` | `"prop:value" \| nodeID` | ∅ | Secondary property index |
| `idx_edge_type` | `"label" \| edgeID` | ∅ | Edge type index |
| `node_labels` | `uint64 nodeID` | MessagePack `[]string` | Node label storage |
| `idx_node_label` | `"label" \| nodeID` | ∅ | Label → node index |
| `meta` | `"node_counter"` / `"edge_counter"` | `uint64` | ID allocation counters |

### Concurrency Model

- **Reads** are fully parallel — `GetNode`, `BFS`, `Cypher`, etc. never acquire a mutex. bbolt's MVCC provides snapshot isolation.
- **Writes** are serialized per-shard by bbolt's single-writer lock.
- The `closed` flag is an `atomic.Bool` — checked by every operation without locking.
- A built-in worker pool (default 8 goroutines) dispatches concurrent queries.

### Sharding

When `ShardCount > 1`, node IDs are hash-partitioned (`nodeID % shardCount`) across separate bbolt files:

- **Edges** are co-located with their source node → `OutEdges(x)` hits **1 shard**.
- **Incoming adjacency** is stored in the target node's shard → `InEdges(x)` hits **1 shard**.
- Cross-shard edge creation uses two separate transactions (two fsyncs) instead of one.

For most use cases, `ShardCount: 1` (default) is sufficient and avoids cross-shard overhead.

## Management UI

GraphDB ships with a built-in web-based management console for exploring your data visually.

### Running the UI

```bash
# Build the React frontend (one-time)
cd ui && npm install && npm run build && cd ..

# Start the server (serves both the API and the UI)
go run ./cmd/graphdb-ui/ -db ./my-data.db -ui ./ui/dist
# → Open http://localhost:7474
```

For development with hot-reload:

```bash
# Terminal 1 — Go API server
go run ./cmd/graphdb-ui/ -db ./my-data.db

# Terminal 2 — React dev server (auto-proxies API calls)
cd ui && npm run dev
# → Open http://localhost:5173
```

### Pages

| Page | What it does |
|---|---|
| **Query Editor** | Write and run Cypher queries with syntax highlighting. Results are shown as a table or as an interactive graph. Includes example queries and keeps a history of past queries. Press `Ctrl+Enter` to execute. |
| **Dashboard** | See your database at a glance — total nodes, edges, shard count, disk usage, and which indexes are active. Quick links to other pages. |
| **Index Management** | Create, drop, or rebuild property indexes through the UI. Each index is shown with its type (B+tree) and status. |
| **Graph Explorer** | Browse all nodes in a paginated list. Click any node to see its properties and a visual graph of its direct connections. Click nodes in the graph to navigate and explore further. |

### REST API

The UI communicates through a JSON API that you can also use directly:

```bash
# Database stats
curl http://localhost:7474/api/stats

# Run a Cypher query
curl -X POST http://localhost:7474/api/cypher \
  -d '{"query": "MATCH (n) RETURN n LIMIT 10"}'

# List indexes
curl http://localhost:7474/api/indexes

# Create an index
curl -X POST http://localhost:7474/api/indexes \
  -d '{"property": "name"}'

# List nodes (paginated)
curl http://localhost:7474/api/nodes?limit=20&offset=0

# Get a node's neighborhood (node + neighbors + edges)
curl http://localhost:7474/api/nodes/1/neighborhood

# Create a node
curl -X POST http://localhost:7474/api/nodes \
  -d '{"props": {"name": "Alice", "age": 30}}'

# Create an edge
curl -X POST http://localhost:7474/api/edges \
  -d '{"from": 1, "to": 2, "label": "follows"}'

# Cursor pagination (O(limit) per page)
curl 'http://localhost:7474/api/nodes/cursor?limit=20'
curl 'http://localhost:7474/api/nodes/cursor?cursor=42&limit=20'
curl 'http://localhost:7474/api/edges/cursor?limit=50'

# Prepare and execute a statement
curl -X POST http://localhost:7474/api/cypher/prepare \
  -d '{"query": "MATCH (n {name: $name}) RETURN n"}'
curl -X POST http://localhost:7474/api/cypher/execute \
  -d '{"stmt_id": "abc123", "params": {"name": "Alice"}}'

# NDJSON streaming
curl -X POST http://localhost:7474/api/cypher/stream \
  -d '{"query": "MATCH (n) RETURN n.name LIMIT 100"}'

# Prometheus metrics
curl http://localhost:7474/metrics

# Query cache stats
curl http://localhost:7474/api/cache/stats
```

### Tech Stack

- **Frontend**: React 18, TypeScript, Vite, Tailwind CSS, [cytoscape.js](https://js.cytoscape.org/) (graph rendering), [CodeMirror](https://codemirror.net/) (query editor), [Lucide](https://lucide.dev/) (icons)
- **Backend**: Go `net/http` with the standard library router (Go 1.22+), no external web framework

## Examples

```bash
# Minimal quickstart
go run ./cmd/graphdb/

# Social network — CRUD, traversals, paths, query builder, concurrency
go run ./examples/social/

# Cypher query patterns — all 7 read patterns + ORDER BY + LIMIT
go run ./examples/cypher/

# Benchmark — 100K nodes, batch insert, index-aware Cypher performance
go run ./examples/benchmark/

# Labels, transactions, parameterized queries
go run ./examples/labels_tx/

# EXPLAIN/PROFILE — query plan inspection and profiling
go run ./examples/explain_profile/

# OPTIONAL MATCH — left-outer-join semantics
go run ./examples/optional_match/

# Data integrity — CRC32 checksums + VerifyIntegrity scan
go run ./examples/integrity/
```

## Benchmarks

Run the built-in benchmarks:

```bash
go test -bench=. -benchmem
```

Or run the 100K-node performance example:

```bash
go run ./examples/benchmark/
```

Typical results on Apple M-series:

| Operation | Throughput |
|---|---|
| AddNodeBatch (100K nodes) | ~120 ms |
| CreateIndex (100K nodes) | ~180 ms |
| FindByProperty (indexed) | < 1 ms |
| Cypher property filter (indexed, 100K) | < 1 ms |
| Cypher 1-hop traversal (indexed) | < 1 ms |
| Cypher ORDER BY + LIMIT 10 (100K) | ~60 ms |
| 1000× repeated Cypher (cached) | ~200 ms |

## Testing

```bash
go test -v ./...
go test -race ./...        # race detector
go test -bench=. -benchmem # benchmarks
```

## Roadmap

### Phase 1 — Foundation
- [ ] **Hot Backup / Restore** — consistent snapshot using bbolt's built-in `WriteTo`, zero downtime
- [ ] **Write-Ahead Log (WAL)** — log every mutation to a sequential file before applying to bbolt; enables replication and point-in-time recovery
- [ ] **Write Cypher** — `CREATE`, `SET`, `DELETE`, `MERGE` support in the Cypher engine
- [ ] **Prometheus Metrics** — `graphdb_nodes_total`, `graphdb_query_duration_seconds`, `graphdb_write_ops_total`, etc.

### Phase 2 — Replication & Reliability
- [ ] **Raft-based Replication** — each shard group runs a Raft consensus group (`hashicorp/raft`) with 1 leader + N followers for automatic failover
- [ ] **Point-in-Time Recovery** — replay WAL from a backup snapshot to restore data to any past timestamp
- [ ] **Change Data Capture (CDC)** — streaming API for external consumers to subscribe to graph mutations in real time
- [ ] **Authentication & TLS** — user/password auth and encrypted connections for network-exposed deployments

### Phase 3 — Distributed Cluster
- [ ] **gRPC Inter-Node Protocol** — `ForwardQuery`, `ForwardWrite`, `TransferShard`, `Heartbeat` RPCs between cluster nodes
- [ ] **Cluster Membership** — node discovery and health checking via gossip protocol (`hashicorp/memberlist`)
- [ ] **Shard Placement Manager** — catalog of shard→node assignments, stored in its own Raft group
- [ ] **Distributed Query Coordinator** — route Cypher queries to the correct node(s), scatter-gather for cross-shard queries, merge results
- [ ] **Distributed Edge Writes** — two-phase commit for edges that span different cluster nodes
- [ ] **Shard Rebalancing & Migration** — move shards between nodes when a node joins or leaves the cluster
- [ ] **Cluster-Aware UI** — topology view, per-node stats, shard distribution map, leader/follower status

### Phase 4 — Production Hardening
- [ ] **Range Indexes** — B+tree range scans for numerical/date properties (`WHERE n.age > 25` without full scan)
- [ ] **Graph Partitioning** — smarter shard placement (METIS/Fennel) to minimize cross-shard edges
- [ ] **Read Replicas** — route read-only Cypher to Raft followers, writes to leader
- [ ] **Bloom Filters** — fast `HasEdge()` checks without touching disk
- [ ] **Schema Constraints** — unique properties, required fields, edge cardinality rules
- [ ] **Query Timeout & Cancellation** — context-based cancellation for long-running queries
- [ ] **Connection Pooling & Rate Limiting** — protect against runaway queries in multi-tenant setups

## License

MIT
