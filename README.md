# GraphDB

A high-performance, embeddable graph database written in Go. Built on top of [bbolt](https://github.com/etcd-io/bbolt) (B+tree key-value store), it supports concurrent queries, secondary indexes, optional hash-based sharding, and a subset of the Cypher query language — all in a single dependency-free binary.

## Features

- **Directed labeled graph** — nodes and edges with arbitrary JSON-like properties  
  `alice ---follows---> bob`, `server ---consumes---> queue`
- **Concurrent reads** — fully parallel BFS, DFS, Cypher, and query-builder calls via MVCC
- **50 GB+ ready** — bbolt memory-mapped storage with configurable `MmapSize`
- **Graph algorithms** — BFS, DFS, Shortest Path (unweighted & Dijkstra), All Paths, Connected Components, Topological Sort
- **Fluent query builder** — chainable Go API with filtering, pagination, and direction control
- **Secondary indexes** — O(log N) property lookups with auto-maintenance on single writes
- **Cypher query language** — read-only subset (7 patterns) with index-aware execution, LIMIT push-down, ORDER BY + LIMIT heap, and query plan caching
- **Batch operations** — `AddNodeBatch` / `AddEdgeBatch` for bulk loading with single-fsync transactions
- **Worker pool** — built-in goroutine pool for concurrent query execution
- **Optional sharding** — hash-based partitioning across multiple bbolt files; edges co-located with source nodes for single-shard traversals
- **Management UI** — built-in web console with a Cypher query editor, interactive graph visualization (cytoscape.js), index management, and a node/edge explorer

## Installation

```bash
go get github.com/mstrYoda/graphdb
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    graphdb "github.com/mstrYoda/graphdb"
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
    res, _ := db.Cypher(`MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)
    for _, row := range res.Rows {
        fmt.Println(row["b.name"]) // Bob
    }
}
```

## Configuration

```go
opts := graphdb.Options{
    ShardCount:     1,              // 1 = single process (default), N = hash-sharded
    WorkerPoolSize: 8,              // goroutines for concurrent query execution
    CacheSize:      100_000,        // LRU cache capacity (node count)
    NoSync:         false,          // true = skip fsync (faster writes, risk of data loss)
    ReadOnly:       false,          // open in read-only mode
    MmapSize:       256 * 1024 * 1024, // 256 MB initial mmap
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
// 1. All nodes
res, _ := db.Cypher(`MATCH (n) RETURN n`)

// 2. Property filter (uses index if available)
res, _ := db.Cypher(`MATCH (n {name: "Alice"}) RETURN n`)

// 3. WHERE clause
res, _ := db.Cypher(`MATCH (n) WHERE n.age > 25 RETURN n`)

// 4. Single-hop pattern match
res, _ := db.Cypher(`MATCH (a)-[:follows]->(b) RETURN a, b`)

// 5. Filtered traversal with property projection
res, _ := db.Cypher(`MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)

// 6. Variable-length path (1 to 3 hops)
res, _ := db.Cypher(`MATCH (a)-[:follows*1..3]->(b) RETURN b`)

// 7. Any edge type with type() function
res, _ := db.Cypher(`MATCH (a)-[r]->(b) RETURN type(r), b`)
```

#### ORDER BY, LIMIT, Prepared Queries

```go
// ORDER BY + LIMIT — uses a min-heap for top-K efficiency
res, _ := db.Cypher(`MATCH (n) WHERE n.age > 20 RETURN n.name ORDER BY n.age DESC LIMIT 5`)

// LIMIT push-down — stops scanning early when no ORDER BY is present
res, _ := db.Cypher(`MATCH (n) RETURN n LIMIT 10`)

// Prepared queries — parse once, execute many times
pq, _ := db.PrepareCypher(`MATCH (n {name: "Alice"})-[:follows]->(b) RETURN b.name`)
res1, _ := db.ExecutePrepared(pq)
res2, _ := db.ExecutePrepared(pq) // no re-parsing

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
│   Node CRUD · Edge CRUD · BFS/DFS · Paths · Query Builder   │
│   Cypher(string) · PrepareCypher · Indexes · Stats           │
├──────────────────────────────────────────────────────────────┤
│                     Cypher Engine                            │
│   Lexer → Parser → AST → Executor (index-aware)             │
│   Query plan cache · LIMIT push-down · Top-K heap           │
├──────────────────────────────────────────────────────────────┤
│                    Shard Manager                             │
│   Hash-based routing · Cross-shard edge handling             │
│   Worker pool for concurrent execution                       │
├──────────────────────────────────────────────────────────────┤
│                   Storage Layer                              │
│   bbolt (B+tree) · Memory-mapped files · MVCC               │
│   Binary encoding · Adjacency-list layout                    │
├──────────────────────────────────────────────────────────────┤
│  Buckets: nodes│edges│adj_out│adj_in│idx_prop│idx_edge_type  │
└──────────────────────────────────────────────────────────────┘
```

### Storage Layout (bbolt buckets)

| Bucket | Key | Value | Purpose |
|---|---|---|---|
| `nodes` | `uint64 nodeID` | JSON properties | Node data |
| `edges` | `uint64 edgeID` | Binary-encoded edge | Edge data (from, to, label, props) |
| `adj_out` | `nodeID \| edgeID` | `targetID \| label` | Outgoing adjacency list |
| `adj_in` | `nodeID \| edgeID` | `sourceID \| label` | Incoming adjacency list |
| `idx_prop` | `"prop:value" \| nodeID` | ∅ | Secondary property index |
| `idx_edge_type` | `"label" \| edgeID` | ∅ | Edge type index |
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

## License

MIT
