# GoraphDB — Production Readiness Checklist

## Phase 1 — Foundation
- [ ] **Hot Backup / Restore** — consistent snapshot using bbolt's built-in `WriteTo`, zero downtime
- [x] **Write-Ahead Log (WAL)** — append-only segmented log (64 MB segments, CRC32, msgpack) with WALReader tailing support; enables replication and point-in-time recovery
- [x] **Write Cypher** — `CREATE`, `SET`, `DELETE`, `MERGE` support in the Cypher engine
- [x] **Prometheus Metrics** — atomic counters for queries, errors, cache hits/misses, node/edge CRUD, index lookups; `/metrics` Prometheus text endpoint + `/api/metrics` JSON endpoint
- [x] **Binary Property Encoding** — replace JSON `encodeProps`/`decodeProps` with MessagePack or custom binary format (3–5× faster, 30–50% smaller)
- [x] **Node Labels as First-Class Concept** — typed labels (`:Person`, `:Movie`) stored in a separate label index with bitmap lookups instead of faking via `props["label"]`
- [x] **User-Facing Transactions (Begin/Commit/Rollback)** — multi-statement atomic operations: `tx := db.Begin(); tx.AddNode(...); tx.AddEdge(...); tx.Commit()`
- [x] **Parameterized Cypher Queries** — `db.CypherWithParams("MATCH (n {name: $name}) RETURN n", params)` to prevent injection and enable plan caching
- [x] **Structured Logging** — integrate `slog` for all write operations, errors, and lifecycle events

## Phase 2 — Replication & Reliability
- [x] **Raft-based Replication** — `hashicorp/raft` for leader election + WAL→gRPC log shipping for data replication; 1 leader + N followers with automatic failover, query router, and HTTP write forwarding
- [ ] **Point-in-Time Recovery** — replay WAL from a backup snapshot to restore data to any past timestamp
- [ ] **Change Data Capture (CDC)** — streaming API for external consumers to subscribe to graph mutations in real time
- [ ] **Authentication & TLS** — user/password auth and encrypted connections for network-exposed deployments
- [x] **Write Cypher (CREATE)** — `CREATE (n:Label {props})`, `CREATE (a)-[:REL]->(b)`, comma-separated patterns, optional RETURN; unified `Cypher()` dispatches read/write automatically
- [ ] **Cypher Aggregations** — `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT` and `WITH` clause for intermediate aggregation/filtering
- [x] **OPTIONAL MATCH** — left-outer-join semantics for graph patterns ("find friends and their posts, even if they have none")
- [x] **Variable-Length Path Patterns** — `(a)-[:KNOWS*1..5]->(b)` execution support with label filtering on source/target nodes
- [x] **EXPLAIN / PROFILE** — return query plan tree and per-operator row counts / timings without/with execution
- [x] **Node/Edge Hot Cache** — sharded concurrent LRU cache for deserialized nodes to avoid repeated bbolt lookups + msgpack decode on hot traversals
- [x] **Data Checksums** — CRC32 (Castagnoli) checksums on node properties and edge blobs, verified on read, with `VerifyIntegrity()` full scan

## Phase 3 — Distributed Cluster
- [x] **gRPC Inter-Node Protocol** — `StreamWAL` server-streaming RPC for WAL replication with auto-reconnect and exponential backoff
- [ ] **Cluster Membership** — node discovery and health checking via gossip protocol (`hashicorp/memberlist`)
- [ ] **Shard Placement Manager** — catalog of shard→node assignments, stored in its own Raft group
- [ ] **Distributed Query Coordinator** — route Cypher queries to the correct node(s), scatter-gather for cross-shard queries, merge results
- [ ] **Distributed Edge Writes** — two-phase commit for edges that span different cluster nodes
- [ ] **Shard Rebalancing & Migration** — move shards between nodes when a node joins or leaves the cluster
- [x] **Cluster-Aware UI** — React cluster dashboard with per-node stats, role indicators, replication progress bars, health status; auto-refresh 5s; aggregator endpoint proxies to all peers
- [ ] **Cost-Based Query Planner** — maintain cardinality estimates (histograms, degree distributions) and choose optimal access methods
- [x] **Composite Indexes** — index on `(label, name)` pairs for single-seek lookups on combined predicates
- [ ] **Full-Text Search Index** — inverted index (or Bleve integration) for `WHERE n.bio CONTAINS "engineer"`
- [x] **Query Result Streaming** — NDJSON streaming via `POST /api/cypher/stream` with `RowIterator`-based lazy evaluation
- [ ] **Binary Wire Protocol (gRPC Service)** — framed binary protocol for programmatic access with connection multiplexing and streaming
- [ ] **Consistency Checker (fsck)** — `goraphdb fsck` tool that validates referential integrity (adj entries ↔ nodes, index entries ↔ data)

## Phase 4 — Production Hardening
- [ ] **Range Indexes** — B+tree range scans for numerical/date properties (`WHERE n.age > 25` without full scan)
- [ ] **Graph Partitioning** — smarter shard placement (METIS/Fennel) to minimize cross-shard edges
- [x] **Read Replicas** — query router routes reads locally, forwards writes to leader via HTTP; `GET /api/health` exposes role for LB-based routing; `GET /api/cluster` for topology introspection
- [x] **Bloom Filters** — in-memory probabilistic filter for fast `HasEdge()` checks (~1.5% FPR, zero false negatives); rebuilt from adj_out on startup; ~1 byte per edge
- [x] **Unique Constraints** — `CreateUniqueConstraint(label, property)` with O(1) lookup via `idx_unique` bucket; enforced on AddNodeWithLabels, UpdateNode, SetNodeProps, DeleteNode; Cypher MERGE uses constraints for efficient upsert
- [x] **Query Timeout & Cancellation** — `CypherContext`/`CypherWithParamsContext` with `context.Context` propagation through all execution strategies; checked at scan loop boundaries
- [x] **Write Backpressure** — bounded write semaphore per shard (`WriteQueueSize`, `WriteTimeout`) with `acquireWrite()`/`releaseWrite()` wrapping all bbolt `Update()` calls; `ErrWriteQueueFull` on timeout
- [x] **Query Governor** — `MaxResultRows` row cap enforced at scan/accumulation boundaries; `DefaultQueryTimeout` applied when caller has no deadline; `ErrResultTooLarge` sentinel
- [x] **Panic Recovery** — `safeExecute()`/`safeExecuteResult()` wrappers with `recover()` + stack trace at all public Cypher entry points; `ErrQueryPanic` sentinel
- [x] **Snapshot Reads** — `DB.Snapshot()` returns consistent point-in-time read view holding per-shard bbolt read txs; `Snapshot.Cypher()`/`CypherWithParams()`; `Snapshot.Release()`
- [ ] **Connection Pooling & Rate Limiting** — protect against runaway queries in multi-tenant setups
- [x] **Compaction / VACUUM** — rewrite bbolt DB file to reclaim space from deletes; `shard.compact()` via snapshot→temp→atomic-rename→reopen; `DB.Compact()` public API; optional background goroutine via `Options.CompactionInterval`
- [x] **Slow Query Log** — configurable `SlowQueryThreshold`, slog warnings, ring buffer of 100 recent entries, `GET /api/slow-queries` endpoint
- [ ] **Query Audit Log** — who ran what, when (compliance)
- [ ] **Runtime-Tunable Config** — change log levels, slow-query thresholds, cache sizes without restart
- [x] **Cursor-Based Pagination** — `ListNodes`/`ListEdges`/`ListNodesByLabel` using bbolt `Cursor.Seek`, O(limit) per page, UI explorer uses cursor navigation

## Phase 5 — Graph Algorithms & Advanced Query
- [ ] **PageRank** — iterative importance scoring
- [ ] **Community Detection** — Louvain / label propagation
- [ ] **Betweenness / Closeness Centrality** — node importance metrics
- [ ] **Triangle Counting / Clustering Coefficient** — structural analysis
- [ ] **K-Hop Neighborhood Count** — count without materializing all nodes
- [ ] **Bidirectional BFS** — faster shortest path on large graphs
- [ ] **UNION / UNION ALL** — combine result sets from multiple MATCH clauses
- [ ] **DISTINCT** — deduplicate result rows
- [ ] **List Comprehensions & UNWIND** — working with collections in results
- [ ] **Index-Only Scans** — skip full node deserialization when RETURN only needs indexed fields
- [ ] **Automatic Index Recommendations** — log slow full-scan queries and suggest indexes

## Phase 6 — Observability & Operations
- [x] **Admin Web UI** — React dashboard with query editor, node explorer, index management, metrics dashboard, slow queries viewer
- [x] **Graceful Shutdown** — SIGTERM/SIGINT handler with ordered teardown: HTTP drain (10s) → Raft/gRPC stop → WAL flush → bbolt close
- [x] **Health Check Endpoint** — `GET /api/health` with role-aware responses (200 OK / 503 Unavailable) for load balancer and orchestrator probes; includes `role` field for intelligent routing
- [ ] **Configurable Logging Levels** — runtime-tunable log verbosity without restart
- [ ] **Disk Usage Reporting** — per-shard size, total DB size, free-page ratio via API

## Phase 7 — Testing & Benchmarks
- [ ] **Fuzz Testing** — fuzz Cypher parser, encoding layer, and random graph mutations (Go 1.18+ native fuzzing)
- [ ] **Linearizability Tests** — concurrent write + read tests asserting no torn reads or lost updates
- [ ] **Crash Recovery Tests** — kill process mid-write, verify DB opens cleanly and is consistent
- [ ] **LDBC Social Network Benchmark** — industry-standard graph DB benchmark for performance tracking
- [ ] **Official Go Client Library** — `graphdb.Client` with retries, connection pooling, and parameter binding
- [ ] **Snapshot Isolation Cross-Shard** — cross-shard snapshot coordinator for consistent multi-shard reads
- [ ] **Deadlock Detection** — wait-for graph or wound-wait prevention for multi-statement cross-shard transactions
