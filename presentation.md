---
marp: true
theme: default
paginate: true
backgroundColor: #0d1117
color: #e6edf3
style: |
  section {
    font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
  }
  h1, h2, h3 {
    color: #58a6ff;
  }
  code {
    background: #161b22;
    color: #79c0ff;
    border-radius: 4px;
    padding: 2px 6px;
  }
  pre {
    background: #161b22 !important;
    border: 1px solid #30363d;
    border-radius: 8px;
  }
  pre code {
    color: #e6edf3;
  }
  table {
    font-size: 0.75em;
  }
  th {
    background: #161b22;
    color: #58a6ff;
  }
  td {
    background: #0d1117;
  }
  a {
    color: #58a6ff;
  }
  blockquote {
    border-left: 4px solid #58a6ff;
    background: #161b22;
    padding: 8px 16px;
    font-style: italic;
    color: #8b949e;
  }
  .columns {
    display: flex;
    gap: 24px;
  }
  .columns > div {
    flex: 1;
  }
  img[alt~="center"] {
    display: block;
    margin: 0 auto;
  }
  em {
    color: #8b949e;
  }
  strong {
    color: #f0883e;
  }
---

<!-- _class: lead -->

# What Is a Graph Database?

A database that uses **nodes**, **edges**, and **properties**
to represent and query data as a **graph**.

---

# Why Graphs?

Relational databases model data as **tables and joins**.
Graph databases model data as **entities and relationships** — directly.

```
  Relational:                    Graph:
  ┌───────┐  ┌──────────┐         (Alice)──follows──>(Bob)
  │ users │──│ follows  │             │                │
  │  id   │  │ from | to│         manages          follows
  │ name  │  │  1   |  2│             ▼                ▼
  └───────┘  └──────────┘         (Charlie)         (Diana)
  + JOIN + JOIN + JOIN...        just traverse edges
```

> When your queries are about **connections**, graphs win.
> No joins. No N+1. Just follow the edges.

---

# Where Are Graph Databases Used?

<div class="columns">
<div>

**Knowledge Graphs**
Entities + relationships at scale
Google Knowledge Graph, Wikidata, enterprise ontologies

**Social Networks**
Friends, followers, recommendations
"People you may know" = 2-hop traversal

**AI Agent Applications**
Tool/memory graphs for LLM agents
RAG with graph-structured knowledge
Reasoning over entity relationships

</div>
<div>

**Fraud Detection**
Suspicious transaction chains
Money laundering ring detection

**Programming & Data Structures**
Dependency graphs, ASTs, call graphs
Compiler IRs, package managers

**Infrastructure & DevOps**
Service meshes, network topologies
Impact analysis ("what breaks if X fails?")

**Recommendation Engines**
"Users who liked X also liked Y"
Content-based + collaborative filtering

</div>
</div>

---

<!-- _class: lead -->

# GoraphDB

### A High-Performance, Embeddable Graph Database in Go

Built on **bbolt** (B+tree) with **Cypher**, **Raft**, and **WAL Replication**

---

# Agenda

1. **Architecture Overview**
2. **Storage Layer & bbolt**
3. **In-Memory, Persistence & Sharding**
4. **Cypher Language**: Lexer, Parser, AST & Execution
5. **Raft, Leader Election & WAL Replication**
6. **Go SDK**
7. **Code Examples**
8. **Other Approaches & What's Next**

---

<!-- _class: lead -->

# 1. Architecture Overview

---

# Architecture — Layered Design

```
┌──────────────────────────────────────────────────────────────┐
│                     Management UI                            │
│   React + TypeScript + Tailwind · cytoscape.js · CodeMirror  │
├──────────────────────────────────────────────────────────────┤
│                    HTTP / JSON API                            │
│   /api/cypher · /api/nodes · /api/edges · /api/indexes       │
├──────────────────────────────────────────────────────────────┤
│                     Replication Layer                         │
│   WAL · gRPC Log Shipping · Applier · Raft Election          │
├──────────────────────────────────────────────────────────────┤
│                     Cypher Engine                             │
│   Lexer → Parser → AST → Executor (index-aware)             │
├──────────────────────────────────────────────────────────────┤
│                    Shard Manager                              │
│   Hash-based routing · Cross-shard edge handling             │
├──────────────────────────────────────────────────────────────┤
│                   Storage Layer (bbolt)                       │
│   B+tree · Memory-mapped · MVCC · MessagePack · CRC32       │
└──────────────────────────────────────────────────────────────┘
```

---

# Architecture — Key Design Decisions

- **Single binary** — no external dependencies, no runtime JVM/CLR
- **Embeddable** — use as a Go library or run as a standalone server
- **bbolt** — battle-tested B+tree KV store (etcd heritage)
- **MVCC** — reads never block writes, fully parallel read path
- **WAL-based replication** — Raft only for leader election, data flows through WAL
- **Cypher** — industry-standard graph query language (Neo4j compatible subset)
- **Kubernetes-ready** — operator, CRDs, Prometheus metrics, graceful shutdown

---

# Project Structure

```
goraphdb/
├── graphdb.go            # DB core: Open, Close, role mgmt
├── storage.go            # bbolt buckets & shard init
├── node.go / edge.go     # CRUD operations
├── cypher_lexer.go       # Tokenizer
├── cypher_parser.go      # Recursive descent parser
├── cypher_ast.go         # Abstract syntax tree
├── cypher_exec.go        # Query executor
├── wal.go                # Write-Ahead Log
├── applier.go            # WAL replay on followers
├── replication/           # Cluster: election, gRPC, router
├── server/               # HTTP API
├── sdk/goraphdb/         # Go client SDK
├── ui/                   # React management UI
└── deployment/k8s/       # Kubernetes operator
```

---

<!-- _class: lead -->

# 2. Storage Layer & bbolt

---

# Why bbolt?

- **B+tree** key-value store — ordered keys, efficient range scans
- **Memory-mapped I/O** — OS manages page cache, supports 50 GB+ datasets
- **MVCC** — snapshot isolation, lock-free reads
- **Single-writer** — serialized writes per file, no WAL needed at KV level
- **Battle-tested** — powers etcd, Consul, InfluxDB
- **Zero dependencies** — pure Go, compiles to static binary

```go
opts := graphdb.Options{
    MmapSize: 256 * 1024 * 1024,  // 256 MB initial mmap
    NoSync:   false,               // fsync on every commit
    ReadOnly: false,
}
```

---

# What Is a B+Tree?

A **self-balancing tree** optimized for storage systems — every database you know uses one (SQLite, PostgreSQL, MySQL InnoDB, etcd, bbolt).

<div class="columns">
<div>

**Binary Search Tree:**
```
        8
       / \
      3   10
     / \    \
    1   6    14
```
- Data in **every** node
- Deep trees = many disk seeks

</div>
<div>

**B+Tree:**
```
      [  3  |  8  ]          ← internal
      /      |      \
  [1,2]   [3,5,6]  [8,10,14] ← leaves
    ↔        ↔        ↔
       linked list
```
- Data **only in leaves**
- Internal nodes = routing keys
- Leaves **linked** for range scans

</div>
</div>

---

# B+Tree — Why It's Perfect for Databases

```
Lookup: key = 6           Range scan: keys 3..10

      [  3  |  8  ]              [  3  |  8  ]
      /      |      \            /      |      \
  [1,2]   [3,5,6]  [8,10,14]  [1,2]  [3,5,6]→[8,10,14]
               ↑                      ↑─────────────↑
              found!                  start          stop
```

---

# B+Tree — Why It's Perfect for Databases

| Property | Benefit |
|---|---|
| **High fan-out** | Shallow tree — 3-4 levels for millions of keys |
| **O(log N) lookup** | ~3 disk reads for 100M keys |
| **Sequential leaves** | Range scans follow the linked list — no tree traversal |
| **Page-aligned** | Each node = one OS page (4 KB) — fits mmap perfectly |
| **Sorted keys** | Prefix scans for indexes (`"name:Alice" \| nodeID`) |

> bbolt stores each **bucket** as a B+tree. GoraphDB has ~10 buckets per shard.

---

# B+Tree — In bbolt & GoraphDB

```
bbolt file (shard_0000.db)
│
├── B+Tree: "nodes"       key: nodeID    → msgpack props + CRC32
├── B+Tree: "edges"       key: edgeID    → binary edge data
├── B+Tree: "adj_out"     key: nodeID|edgeID → targetID|label
├── B+Tree: "adj_in"      key: nodeID|edgeID → sourceID|label
├── B+Tree: "idx_prop"    key: "name:Alice"|nodeID → ∅
└── ...
```

---

**How GoraphDB uses B+tree properties:**
- **Point lookup** (`GetNode(42)`) → O(log N) seek by nodeID key
- **Adjacency scan** (`OutEdges(42)`) → prefix scan on `adj_out` where key starts with nodeID
- **Index seek** (`FindByProperty("name","Alice")`) → prefix scan on `idx_prop`
- **Label scan** (`FindByLabel("Person")`) → prefix scan on `idx_node_label`

> All O(log N) seek + O(K) sequential scan. No hash tables, no full scans.

---

# bbolt Bucket Layout

| Bucket | Key | Value | Purpose |
|---|---|---|---|
| `nodes` | `uint64 nodeID` | msgpack + CRC32 | Node properties |
| `edges` | `uint64 edgeID` | binary header + props | Edge data |
| `adj_out` | `nodeID \| edgeID` | `targetID \| label` | Outgoing adjacency |
| `adj_in` | `nodeID \| edgeID` | `sourceID \| label` | Incoming adjacency |
| `idx_prop` | `prop:value \| nodeID` | - | Secondary index |
| `idx_node_label` | `label \| nodeID` | - | Label index |
| `idx_unique` | `label\0prop\0value` | nodeID | Unique constraints |
| `node_labels` | `uint64 nodeID` | msgpack `[]string` | Labels |
| `meta` | counter name | `uint64` | ID allocation |

---

# What Is MVCC?

**MVCC** (Multi-Version Concurrency Control) — instead of locking data, keep **multiple versions** so readers and writers don't block each other.

```
 Traditional locking:               MVCC:

 Writer ──► LOCK row ──► write      Writer ──► create new version
 Reader ──► WAIT... ──► blocked     Reader ──► read old version ✓
                                                (no waiting!)
```

| Approach | Readers block writers? | Writers block readers? |
|---|---|---|
| **Locking** | Yes | Yes |
| **MVCC** | **No** | **No** |

> Used by PostgreSQL, SQLite (WAL mode), MySQL InnoDB, bbolt, and GoraphDB.

---

# Snapshot Isolation — How MVCC Works

Each transaction sees a **consistent snapshot** of the database at the moment it started — regardless of concurrent writes.

```
 Time ─────────────────────────────────────────────►

 TX1 (write):  begin ──► update Alice.age=31 ──► commit
                              │
 TX2 (read):       begin ─────┼──► read Alice ──► age=30 ✓
                              │         ↑
                              │    sees snapshot from
                              │    before TX1 committed
                              │
 TX3 (read):                  │              begin ──► read Alice ──► age=31 ✓
                              │                             ↑
                              │                      sees snapshot after
                              │                      TX1 committed
```

> No dirty reads, no phantom reads — every reader gets a **frozen-in-time** view.

---

# MVCC in bbolt — Copy-on-Write B+Trees

When a **write TX** modifies a leaf, bbolt doesn't overwrite in place.
It **copies the dirty path** from leaf to root, leaving old pages intact:

```
  BEFORE write:                     DURING write TX:

       [Root]                            [Root']  ← new copy
       /    \                            /    \
    [A]     [B]                       [A]    [B'] ← new copy
    / \     / \                       / \    / \
  [1] [2] [3] [4]                  [1] [2] [3] [4'] ← modified leaf
                                                 ↑
                                          only this changed
```

- Pages **[A], [1], [2], [3]** are **shared** — not copied
- Only **3 pages** written (leaf + ancestors) out of 7 total
- Old root still valid → active read TXs keep seeing it

---

# Copy-on-Write — Commit & Reclaim

```
  ① Write TX creates new pages:     [Root'] → [B'] → [4']
  ② Old pages still referenced:     [Root]  → [B]  → [4]
     by active read TXs

  ③ Commit: atomically swap the root pointer
     meta page: root = Root'        (single 4KB page write + fsync)

  ④ After all read TXs on old root close:
     [Root], [B], [4] → added to bbolt's FREELIST
     (reused by future writes, not deleted)
```

---

**Does it increase disk usage?**

| Concern | Reality |
|---|---|
| Temporary bloat | Old pages live until read TXs close |
| Freelist reclaim | bbolt reuses freed pages — file doesn't grow endlessly |
| Worst case | Long-running read TX holds old pages → file grows temporarily |
| Mitigation | Keep read TXs short; GoraphDB's Cypher uses scoped read TXs |
| Compaction | bbolt can be compacted offline (`bbolt compact`) if needed |

> Typical overhead: **< 5%** extra disk. The tradeoff: **zero read locks**.

---

# Copy-on-Write — Why It's Worth It

<div class="columns">
<div>

**In-place update (traditional):**
```
Writer: lock page → modify → unlock
Reader: WAIT for lock... blocked

  Writer ████████░░░░░░
  Reader ░░░░░░░░████░░  (delayed)
```

- Simple, but serialized
- Readers wait for writers
- Risk of corruption on crash

</div>
<div>

**Copy-on-write (bbolt):**
```
Writer: copy path → modify copy → commit
Reader: read old pages freely

  Writer ████████░░░░░░
  Reader ██████████████  (parallel!)
```

- Old pages = stable snapshot
- Readers never wait
- Crash-safe: old root valid until commit

</div>
</div>

> This is why GoraphDB can run **50 parallel Cypher queries** while a write commits.

---

# MVCC in bbolt & GoraphDB — Summary

**GoraphDB concurrency model:**
- **Reads** (`GetNode`, `BFS`, `Cypher`) → bbolt **read TX** → fully parallel, never block
- **Writes** (`AddNode`, `AddEdge`) → bbolt **write TX** → serialized per shard
- `db.closed` is `atomic.Bool` — checked without any lock
- Worker pool (default 8) dispatches concurrent read queries

**With sharding, writes also parallelize:**
```
  Shard 0: write TX ──► [shard_0000.db]     (independent lock)
  Shard 1: write TX ──► [shard_0001.db]     (independent lock)
  Shard 2: write TX ──► [shard_0002.db]     (independent lock)
  Shard 3: write TX ──► [shard_0003.db]     (independent lock)
```

> 4 shards = up to **4 concurrent writers** + unlimited concurrent readers.

---

# MVCC in Action — GoraphDB Example

```go
// These run in PARALLEL — no blocking, no waiting:
go func() {
    // Read TX 1: sees consistent snapshot
    res, _ := db.Cypher(ctx, `MATCH (n:Person) RETURN n`)
}()

go func() {
    // Read TX 2: sees its own consistent snapshot
    results, _ := db.BFSCollect(alice, 3, graphdb.Outgoing)
}()

go func() {
    // Write TX: serialized per shard, doesn't block readers
    db.AddNode(graphdb.Props{"name": "NewPerson"})
    // readers above DON'T see NewPerson (started before commit)
}()

go func() {
    // Read TX 3: starts AFTER write commits → SEES NewPerson
    node, _ := db.FindByProperty("name", "NewPerson")
}()
```

> 50 concurrent Cypher queries + 1 writer = **all execute without waiting**.

---

# Encoding: MessagePack + CRC32

**Why MessagePack over JSON?**
- **3-5x faster** serialization/deserialization
- **30-50% smaller** on disk
- Backward-compatible format detection (magic byte `0x02`)

```
Node Record:
┌──────────┬─────────────────────┬──────────┐
│ 0x02     │  MessagePack props  │ CRC32    │
│ (magic)  │  {name: "Alice"...} │ checksum │
└──────────┴─────────────────────┴──────────┘

Edge Record:
┌────┬──────┬────┬───────────┬───────┬──────────────┬───────┐
│ ID │ From │ To │ Label Len │ Label │ msgpack props│ CRC32 │
│ 8B │  8B  │ 8B │   2B      │ var   │     var      │  4B   │
└────┴──────┴────┴───────────┴───────┴──────────────┴───────┘
```

---

# Data Integrity

Every read verifies CRC32 checksums. Full database verification available:

```go
report, err := db.VerifyIntegrity()

fmt.Printf("Checked %d nodes, %d edges\n",
    report.NodesChecked, report.EdgesChecked)

if report.OK() {
    fmt.Println("All data intact!")
} else {
    for _, e := range report.Errors {
        fmt.Println(e)
        // "shard 0, nodes[00000001]: checksum mismatch"
    }
}
```

CRC32 Castagnoli — hardware-accelerated on modern CPUs.

---

<!-- _class: lead -->

# 3. In-Memory, Persistence & Sharding

---

# In-Memory Components

| Component | Purpose | Size |
|---|---|---|
| **Node LRU Cache** | Hot node reads | ~128 MB budget |
| **Bloom Filter** | Fast `HasEdge()` negative | ~1 byte/edge |
| **Query Plan Cache** | Skip re-parsing | 10K entries LRU |
| **Atomic Counters** | Node/edge counts, next IDs | 64-bit atomic |
| **Index Metadata** | Track active indexes | `sync.Map` |

```go
opts := graphdb.Options{
    CacheBudget:    128 * 1024 * 1024,  // 128 MB LRU
    WorkerPoolSize: 8,                   // concurrent queries
}
```

---

# What Is a Bloom Filter?

A **space-efficient probabilistic data structure** that answers: _"Is this element in the set?"_

```
Insert edge (Alice → Bob):  hash₁=3, hash₂=7, hash₃=11, hash₄=14

Bit array:  [0 0 0 1 0 0 0 1 0 0 0 1 0 0 1 0]
                   ↑       ↑       ↑     ↑
                   3       7       11    14

Query: HasEdge(Alice → Bob)?   → check bits 3,7,11,14 → all 1 → "MAYBE"
Query: HasEdge(Alice → Eve)?   → check bits 2,5,9,13  → bit 2=0 → "DEFINITELY NOT"
```

---

# What Is a Bloom Filter?


| Answer | Meaning |
|---|---|
| **"Definitely not"** | 100% certain — skip the disk read |
| **"Maybe yes"** | ~1.5% chance of false positive — check bbolt to confirm |



> GoraphDB: **k=4 FNV hashes**, ~1 byte/edge, rebuilt from `adj_out` on startup.
> Saves thousands of disk I/O calls on dense graphs.

---

# Persistence Model

```
data/
├── shard_0000.db       ← bbolt file (B+tree, mmap'd)
├── shard_0001.db
├── shard_0002.db
├── shard_0003.db
├── wal-0000000000.log  ← WAL segment (64 MB, CRC32)
├── wal-0000000001.log
└── wal-0000000002.log
```

- **Durability**: fsync on every bbolt commit (configurable `NoSync`)
- **WAL segments**: rotated at 64 MB, CRC32 per frame
- **Group commit**: batched fsync every 2ms — eliminates per-write bottleneck
- **Recovery**: WAL replay from last known-good LSN

---

# Sharding

Hash-based partitioning: `shardFor(id) = shards[id % shardCount]`

```
                    ┌─────────────┐
     nodeID=1  ───► │ shard_0001  │  (1 % 4 = 1)
     nodeID=5  ───► │             │  (5 % 4 = 1)
                    └─────────────┘

                    ┌─────────────┐
     nodeID=2  ───► │ shard_0002  │  (2 % 4 = 2)
     nodeID=6  ───► │             │  (6 % 4 = 2)
                    └─────────────┘
```

---

**Edge placement:**
- **Outgoing adjacency** (`adj_out`) → stored with **source** node's shard
- **Incoming adjacency** (`adj_in`) → stored with **target** node's shard
- `OutEdges(x)` → **1 shard** hit
- `InEdges(x)` → **1 shard** hit
- Cross-shard edge → **2 transactions** (two fsyncs)

---

# Node Cache — Sharded LRU

**LRU** (Least Recently Used) — when the cache is full, **evict the item that hasn't been accessed for the longest time**.

Let's trace access order: **A B C D A E** with capacity = 4.

---

# LRU Step-by-Step — ① add A

```
  ┌───┬───┬───┬───┐
  │ A │   │   │   │         size: 1/4
  └───┴───┴───┴───┘
    ↑
   hot
```

New item **A** enters the cache at the front (hot end).

---

# LRU Step-by-Step — ② add B

```
  ┌───┬───┬───┬───┐
  │ B │ A │   │   │         size: 2/4
  └───┴───┴───┴───┘
    ↑
   hot
```

New item **B** pushes to the front. A slides right.

---

# LRU Step-by-Step — ③ add C

```
  ┌───┬───┬───┬───┐
  │ C │ B │ A │   │         size: 3/4
  └───┴───┴───┴───┘
    ↑
   hot
```

New item **C** at the front. Everything shifts right.

---

# LRU Step-by-Step — ④ add D (FULL)

```
  ┌───┬───┬───┬───┐
  │ D │ C │ B │ A │         size: 4/4  ← FULL
  └───┴───┴───┴───┘
    ↑               ↑
   hot            cold
```

Cache is now **full**. Next insert must **evict** the cold end.

---

# LRU Step-by-Step — ⑤ access A

A is already in cache — move it to the **hot end**:

```
  BEFORE:
  ┌───┬───┬───┬───┐
  │ D │ C │ B │ A │         A found at cold end
  └───┴───┴───┴───┘
                 ↑ found

  AFTER:
  ┌───┬───┬───┬───┐
  │ A │ D │ C │ B │         A moved to front
  └───┴───┴───┴───┘
    ↑            ↑
   hot          cold
```

Now **B** is the least recently used (cold end).

---

# LRU Step-by-Step — ⑥ add E (evict B)

Cache is full — evict **B** (cold end), insert **E** at front:

```
  BEFORE:
  ┌───┬───┬───┬───┐
  │ A │ D │ C │ B │         B = least recently used
  └───┴───┴───┴───┘
                 ↑ evict

  AFTER:
  ┌───┬───┬───┬───┐
  │ E │ A │ D │ C │         B gone, E at front
  └───┴───┴───┴───┘
    ↑            ↑
   hot          cold
```

> Implemented as **doubly-linked list + hash map** → O(1) get, O(1) put, O(1) evict.

---

# Node Cache — Sharded LRU

GoraphDB shards the LRU into **16 independent caches** to reduce lock contention:

```
           ┌──────────┐  ┌──────────┐       ┌──────────┐
           │ Shard 0  │  │ Shard 1  │  ...  │ Shard 15 │
           │   LRU    │  │   LRU    │       │   LRU    │
           └──────────┘  └──────────┘       └──────────┘
                    ▲
                    │  nodeID % 16
                    │
              cache.Get(nodeID)
```

- **Byte-budgeted** eviction (128 MB default) — not entry count
- Prometheus: `graphdb_cache_hits_total`, `graphdb_cache_misses_total`

---

<!-- _class: lead -->

# 4. Cypher Language Implementation

---

# Cypher Pipeline

```
  Query String
       │
       ▼
  ┌─────────┐     ┌──────────┐    ┌─────────┐    ┌───────────┐
  │  Lexer  │────►│  Parser  │───►│   AST   │───►│ Executor  │
  │ (tokens)│     │ (r.d.)   │    │ (tree)  │    │ (index-   │
  └─────────┘     └──────────┘    └─────────┘    │  aware)   │
                                                 └─────┬─────┘
                                                       │
                                    ┌──────────┐       │
                                    │  Cache   │◄──────┘
                                    │ (10K LRU)│
                                    └──────────┘
                                           │
                                           ▼
                                       Results
```

Supports: `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `SKIP`, `LIMIT`,
`CREATE`, `MERGE`, `SET`, `DELETE`, `OPTIONAL MATCH`, `EXPLAIN`, `PROFILE`

---

# Quick Primer: How Languages Are Built

Every query language (SQL, Cypher, GraphQL) follows the same pipeline
that compilers and interpreters use since the 1960s:

```
 ① Define the grammar    (BNF / EBNF)
 ② Tokenize the input    (Lexer)
 ③ Build a syntax tree   (Parser → AST)
 ④ Walk the tree          (Executor / Evaluator)
```

Let's look at each concept before diving into GoraphDB's implementation.

---

# What Is BNF?

**BNF** (Backus-Naur Form) — a notation for describing a language's grammar.

```bnf
<query>       ::= <match> <where>? <return>
<match>       ::= "MATCH" <pattern>
<pattern>     ::= <node> ( <rel> <node> )*
<node>        ::= "(" <ident>? (":" <label>)? <props>? ")"
<rel>         ::= "-[" <ident>? (":" <label>)? "]->"
                 | "<-[" <ident>? (":" <label>)? "]-"
<where>       ::= "WHERE" <expression>
<return>      ::= "RETURN" <return_item> ("," <return_item>)*
<expression>  ::= <prop_access> <operator> <literal>
```

Each rule defines what is **valid syntax**.
The parser's job is to verify the input matches these rules and build a tree.

> GoraphDB doesn't use a parser generator — the BNF is encoded directly in Go functions.

---

# What Is a Lexer (Tokenizer)?

The **lexer** is the first pass. It reads raw characters and groups them into **tokens** — the smallest meaningful units of the language.

> Strips whitespace, comments, recognizes keywords vs identifiers vs literals.

<div class="columns">
<div>

**Input** (characters):
```
MATCH (a:Person{name: "Alice"})
```

</div>
<div>

**Output** (tokens):
```
tokMatch
tokLParen
tokIdent("a")
tokColon
tokIdent("Person")
tokLBrace
tokIdent("name")
tokColon
tokString("Alice")
tokRBrace
tokRParen
```

</div>
</div>

---

# What Is a Parser?

The **parser** reads the token stream and checks it against the grammar rules.
If valid, it produces a **tree structure** representing the query.

```
  Tokens:  [MATCH, (, a, :, Person, ..., ), RETURN, b, ., name]
                │
                ▼
           ┌──────────┐
           │  Parser  │    Checks: is this valid Cypher?
           │          │    Builds: structured representation
           └────┬─────┘
                │
                ▼
            AST (tree)
```

If the tokens **don't** match the grammar → **syntax error**.
If they **do** match → we get a tree we can execute.

---

# What Is an AST?

**AST** (Abstract Syntax Tree) — a tree representation of the query's structure, stripped of syntactic sugar (parentheses, commas, keywords).

```
MATCH (a:Person)-[:follows]->(b) RETURN b.name

          CypherQuery
          ├── MatchClause
          │   └── Pattern
          │       ├── Node(a, :Person)
          │       ├── Rel(:follows, →)
          │       └── Node(b)
          └── Return
              └── PropAccess(b, "name")
```

The AST captures **meaning**, not syntax:
- No parentheses, no commas, no arrow characters
- Just: "match this pattern, return that property"

> The executor walks this tree to plan and run the query.

---

# What Is a Recursive Descent Parser?

A parsing technique where **each grammar rule becomes a function**:

```go
// BNF:  <query> ::= <match> <where>? <return>
func (p *parser) parseQuery() *CypherQuery {
    match  := p.parseMatch()     // consume MATCH ...
    where  := p.maybeWhere()     // consume WHERE ... (optional)
    ret    := p.parseReturn()    // consume RETURN ...
    return &CypherQuery{Match: match, Where: where, Return: ret}
}

// BNF:  <node> ::= "(" <ident>? (":" <label>)? <props>? ")"
func (p *parser) parseNode() *NodePattern {
    p.expect(tokLParen)          // must see "("
    name := p.maybeIdent()       // optional variable
    label := p.maybeLabel()      // optional :Label
    props := p.maybeProps()      // optional {k: v}
    p.expect(tokRParen)          // must see ")"
    return &NodePattern{Var: name, Labels: label, Props: props}
}
```

**Recursive** = rules can call other rules (and themselves).
**Descent** = starts from the top rule and descends into sub-rules.

> No parser generator (yacc, ANTLR) needed — just Go functions.

---

# Lexer — `cypher_lexer.go`

Transforms query string into a stream of typed tokens:

```go
// Token types
tokMatch, tokReturn, tokWhere, tokCreate, tokMerge, tokSet,
tokDelete, tokOptional, tokExplain, tokProfile,
tokIdent, tokString, tokNumber, tokParam,  // $name
tokArrowRight, tokArrowLeft,               // -> <-
tokDotDot,                                 // ..  (var-length)
tokLParen, tokRParen, tokLBracket, tokRBracket,
tokColon, tokDot, tokComma, tokStar,
tokEq, tokNeq, tokLt, tokGt, tokLte, tokGte,
tokAnd, tokOr, tokNot,
tokOrderBy, tokAsc, tokDesc, tokLimit, tokSkip, tokAs
```

---

# Parser — `cypher_parser.go`

**Recursive descent** parser — no external parser generator.

```
parsedCypher
├── read:  CypherQuery   (MATCH ... RETURN)
├── write: CypherWrite   (CREATE)
└── merge: CypherMerge   (MERGE ... ON CREATE SET / ON MATCH SET)
```

Handles three kinds of statements:
1. **Read queries** — `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `LIMIT`, `SKIP`
2. **Write queries** — `CREATE (n:Label {props})-[:REL]->(m)`
3. **Merge queries** — `MERGE (n:Label {props}) ON CREATE SET ...`

> Parameterized queries: `$param` tokens resolved at execution time

---

# AST — `cypher_ast.go`

```
CypherQuery
├── MatchClause
│   └── Patterns[]
│       ├── NodePattern { Variable, Labels[], Props{} }
│       └── RelPattern  { Variable, Label, Direction, VarLength }
├── Where (Expression tree)
│   ├── ComparisonExpr { Left, Op, Right }
│   ├── AndExpr / OrExpr / NotExpr
│   ├── PropAccess { Variable, Property }
│   ├── FuncCall { Name, Args[] }
│   └── Literal / ParamRef
├── OptionalMatch[]
├── Set[]  / Delete[]
├── Return { Items[], OrderBy[], Skip, Limit }
└── Flags: Explain, Profile
```

---

<!-- _class: lead -->

# Deep Dive: A Query Through the Pipeline

Let's trace a real query step by step

```cypher
MATCH (a:Person {name: "Alice"})-[:follows]->(b)
WHERE b.age > 25
RETURN b.name, b.age
ORDER BY b.age DESC LIMIT 3
```

---

# Step 1 — Raw Query String

The user submits a Cypher query:

```
╔══════════════════════════════════════════════════════════════════╗
║  MATCH (a:Person {name: "Alice"})-[:follows]->(b)              ║
║  WHERE b.age > 25                                              ║
║  RETURN b.name, b.age                                          ║
║  ORDER BY b.age DESC LIMIT 3                                   ║
╚══════════════════════════════════════════════════════════════════╝
                              │
                              ▼
                        ┌───────────┐
                        │   Lexer   │
                        └───────────┘
```

The **lexer** scans left-to-right, character by character, producing tokens.

---

# Step 2 — Lexer Output: Token Stream

```
 MATCH (a:Person {name: "Alice"})-[:follows]->(b) WHERE b.age > 25 ...
 ─┬──  ┬┬┬────── ┬┬───  ┬──────┬┬ ┬┬──────┬ ┬─┬┬ ┬──── ┬┬───┬┬──
  │    │││       ││     │      ││  ││      │ │ ││ │     ││   ││
  │    │││       ││     │      ││  ││      │ │ ││ │     ││   │└ tokNumber(25)
  │    │││       ││     │      ││  ││      │ │ ││ │     ││   └─ tokGt
  │    │││       ││     │      ││  ││      │ │ ││ │     │└──── tokDot
  │    │││       ││     │      ││  ││      │ │ ││ │     └───── tokIdent("age")
  │    │││       ││     │      ││  ││      │ │ ││ └── tokWhere
  │    │││       ││     │      ││  ││      │ │ │└── tokIdent("b")
  │    │││       ││     │      ││  ││      │ │ └─── tokLParen
  │    │││       ││     │      ││  ││      │ └──── tokArrowRight (->)
  │    │││       ││     │      ││  ││      └────── tokRBracket
  │    │││       ││     │      ││  │└── tokIdent("follows")
  │    │││       ││     │      ││  └─── tokColon
  │    │││       ││     │      │└────── tokLBracket
  │    │││       ││     └──────└─────── tokRBrace, tokRParen, tokDash
  │    │││       │└── tokString("Alice")
  │    ││└────── └─── tokIdent("name"), tokColon
  │    │└── tokColon, tokIdent("Person"), tokLBrace
  │    └─── tokLParen, tokIdent("a")
  └── tokMatch
```

---

# Step 2 — Lexer Output: Simplified

The token stream as a flat list:

```
[ tokMatch,
  tokLParen, tokIdent("a"), tokColon, tokIdent("Person"),
    tokLBrace, tokIdent("name"), tokColon, tokString("Alice"), tokRBrace,
  tokRParen,
  tokDash,
  tokLBracket, tokColon, tokIdent("follows"), tokRBracket,
  tokArrowRight,
  tokLParen, tokIdent("b"), tokRParen,

  tokWhere,
    tokIdent("b"), tokDot, tokIdent("age"), tokGt, tokNumber(25),

  tokReturn,
    tokIdent("b"), tokDot, tokIdent("name"), tokComma,
    tokIdent("b"), tokDot, tokIdent("age"),

  tokOrderBy,
    tokIdent("b"), tokDot, tokIdent("age"), tokDesc,
  tokLimit, tokNumber(3),
  tokEOF ]
```

---

# Step 3 — Parser Consumes Tokens

The **recursive descent parser** walks the token stream:

```
parseQuery()
│
├─ saw tokMatch → parseMatchClause()
│   └─ parsePattern()
│       ├─ parseNodePattern()  ← (a:Person {name:"Alice"})
│       ├─ parseRelPattern()   ← -[:follows]->
│       └─ parseNodePattern()  ← (b)
│
├─ saw tokWhere → parseWhereClause()
│   └─ parseExpression()
│       └─ parseComparison()   ← b.age > 25
│
├─ saw tokReturn → parseReturnClause()
│   ├─ parseReturnItem()       ← b.name
│   └─ parseReturnItem()       ← b.age
│
├─ saw tokOrderBy → parseOrderBy()
│   └─ b.age DESC
│
└─ saw tokLimit → parseLimit()
    └─ 3
```

---

# Step 4 — AST Generated

```
CypherQuery
├── MatchClause
│   └── Pattern[0]
│       ├── NodePattern { Var:"a", Labels:["Person"],
│       │                 Props:{name:"Alice"} }
│       ├── RelPattern  { Label:"follows", Dir:Outgoing }
│       └── NodePattern { Var:"b" }
│
├── Where
│   └── ComparisonExpr
│       ├── Left:  PropAccess { Var:"b", Prop:"age" }
│       ├── Op:    ">"
│       └── Right: Literal { Value: 25 }
│
├── Return
│   ├── Items: [ PropAccess(b.name), PropAccess(b.age) ]
│   ├── OrderBy: [ { Expr: PropAccess(b.age), Desc: true } ]
│   └── Limit: 3
│
└── Flags: { Explain: false, Profile: false }
```

---

# Step 4b — How the AST Becomes Executable Code

The AST is **not compiled** — it's a Go struct tree that the executor **walks at runtime**.
Each AST node type maps to a Go function call:

```go
func (db *DB) executeCypherNormal(ctx, q *CypherQuery) {
    pat := q.Match.Pattern

    switch {
    case len(pat.Nodes)==1 && len(pat.Rels)==0:
        // MATCH (n)  or  MATCH (n {prop: val})
        return db.execNodeMatch(ctx, q)       // ← Strategy 1

    case len(pat.Nodes)==2 && len(pat.Rels)==1:
        if rel.VarLength {
            return db.execVarLengthMatch(ctx, q)  // ← Strategy 3
        }
        return db.execSingleHopMatch(ctx, q)      // ← Strategy 2
    }
}
```

The executor reads the **shape** of the AST (how many nodes, how many rels, variable-length?) and picks a strategy — then each strategy reads deeper AST fields (labels, props, WHERE) to decide scanning.

---

# Step 4c — AST Fields Drive Every Decision

```
CypherQuery AST                       Executor reads...
─────────────────                     ──────────────────
Match.Pattern.Nodes[0]
  .Labels = ["Person"]          ───►  pick label scan vs full scan
  .Props  = {name: "Alice"}     ───►  pick index seek vs filter

Match.Pattern.Rels[0]
  .Label  = "follows"           ───►  filter edges by label
  .Dir    = Outgoing            ───►  scan adj_out (not adj_in)
```

---

```
CypherQuery AST                       Executor reads...
─────────────────                     ──────────────────

Where
  .ComparisonExpr               ───►  evalExpr() on each candidate
  .Op = ">"                     ───►  compare property value

Return
  .OrderBy = [b.age DESC]       ───►  use top-K heap (not full sort)
  .Limit   = 3                  ───►  heap size = 3, stop early
```

> The AST is a **data structure**. The executor is **Go code that interprets it**.
> No bytecode, no VM — just struct field reads and bbolt transactions.

---

# Step 5 — Executor: Plan Selection

The executor inspects the AST and picks **scan strategies**:

```
 Node "a":  Labels=["Person"], Props={name:"Alice"}
 ──────────────────────────────────────────────────
   1. Unique constraint on (Person, name)?  → NO
   2. Composite index on (Person, name)?    → NO
   3. Property index on "name"?             → YES ✓
   ────────────────────────────────
   Strategy: PropertyIndexSeek("name", "Alice")
             then filter by label :Person

 Node "b":  no label, no props
 ──────────────────────────────────────────────────
   Resolved via edge traversal from "a"
   Strategy: ExpandFromNode(a, "follows", Outgoing)
```

> ORDER BY + LIMIT 3 → **Top-K min-heap** (never sorts full result)

---

# Step 6 — Executor: Scan & Filter

```
PropertyIndexSeek("name", "Alice")
│
│  idx_prop bucket: "name:Alice" | nodeID=1  ← HIT
│
▼
Node 1: {name:"Alice", age:30, labels:["Person"]}  ✓ label match
│
│  ExpandFromNode(1, "follows", Outgoing)
│  adj_out bucket: prefix scan nodeID=1
│
├──► Edge→Node 2: {name:"Bob",     age:25}  → WHERE 25 > 25?  ✗ SKIP
├──► Edge→Node 3: {name:"Charlie", age:35}  → WHERE 35 > 25?  ✓ PUSH HEAP
├──► Edge→Node 5: {name:"Eve",     age:32}  → WHERE 32 > 25?  ✓ PUSH HEAP
└──► Edge→Node 4: {name:"Diana",   age:28}  → WHERE 28 > 25?  ✓ PUSH HEAP
```

Top-3 heap (DESC by age): **Charlie(35), Eve(32), Diana(28)**

---

# Step 7 — Final Result

```
  Heap extracted → sorted DESC → projected:

  ┌─────────┬───────┐
  │ b.name  │ b.age │
  ├─────────┼───────┤
  │ Charlie │   35  │
  │ Eve     │   32  │
  │ Diana   │   28  │
  └─────────┴───────┘

  Execution stats:
    Index seeks:    1  (PropertyIndexSeek)
    Edges scanned:  4  (adj_out prefix scan)
    Nodes filtered: 1  (Bob, age=25)
    Heap ops:       3  (push only, no full sort)
    Result rows:    3
```

> Cached in **query plan LRU** (10K entries) — next identical query skips parsing.

---

# Full Pipeline — Animated Recap

```
"MATCH (a:Person {name:'Alice'})-[:follows]->(b) WHERE b.age>25 ..."
  │
  │ ① LEXER
  ▼
[tokMatch, tokLParen, tokIdent("a"), tokColon, tokIdent("Person"), ...]
  │
  │ ② PARSER (recursive descent)
  ▼
CypherQuery { Match, Where, Return, OrderBy, Limit }
  │
  │ ③ PLAN SELECTION (index-aware)
  ▼
PropertyIndexSeek → ExpandFromNode → Filter → TopKHeap
  │
  │ ④ EXECUTION (scan + filter + heap)
  ▼
┌─────────┬───────┐
│ Charlie │   35  │    ← 3 rows, sub-millisecond
│ Eve     │   32  │
│ Diana   │   28  │
└─────────┴───────┘
```

---

# AST — Pattern Matching

<div class="columns">
<div>

**Cypher:**
```cypher
MATCH (a:Person {name: "Alice"})
  -[r:follows*1..3]->
  (b:Person)
```

</div>
<div>

**Parsed AST:**
```
├── NodePattern
│     Var: "a", Labels: ["Person"]
│     Props: {name: "Alice"}
├── RelPattern
│     Var: "r", Label: "follows"
│     Dir: Outgoing (→)
│     VarLength: {Min:1, Max:3}
└── NodePattern
      Var: "b", Labels: ["Person"]
```

</div>
</div>

---

# Executor — Index-Aware Query Planning

The executor picks the **cheapest scan strategy** for each pattern:

| Strategy | When Used | Cost |
|---|---|---|
| **UniqueConstraintSeek** | Label + property with unique constraint | O(1) |
| **CompositeIndexSeek** | Multi-property match with composite index | O(log N) |
| **PropertyIndexSeek** | Single property match with index | O(log N) |
| **NodeByLabelScan** | `:Label` pattern, label index exists | O(label size) |
| **AllNodesScan** | No indexes match | O(N) |

**Optimizations:**
- **LIMIT push-down** — stops scanning early (no `ORDER BY`)
- **Top-K heap** — `ORDER BY + LIMIT` uses min-heap, not full sort
- **Context cancellation** — checked at every iteration point

---

# EXPLAIN / PROFILE

```go
res, _ := db.Cypher(ctx,
    `EXPLAIN MATCH (n:Person) WHERE n.age > 25 RETURN n`)
```

```
EXPLAIN:
└── ProduceResults (n)
    └── Filter (WHERE clause)
        └── NodeByLabelScan (n:Person)
```

```go
res, _ := db.Cypher(ctx,
    `PROFILE MATCH (n:Person) RETURN n`)
```

```
PROFILE:
└── ProduceResults (n) [rows=42, time=150µs]
    └── NodeByLabelScan (n:Person) [rows=42]
```

> `EXPLAIN` = zero I/O plan inspection. `PROFILE` = actual execution with timing.

---

<!-- _class: lead -->

# 5. Raft, Leader Election & WAL Replication

---

# Replication Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Replication Cluster                    │
│                                                             │
│  ┌──────────────┐   gRPC StreamWAL   ┌──────────────┐       │
│  │    Leader    │ ────────────────►  │  Follower 1  │       │
│  │              │                    │              │       │
│  │  Writes──►WAL│ ────────────────►  │  Applier──►DB│       │
│  │              │   gRPC StreamWAL   ┌──────────────┐       │
│  │              │ ────────────────►  │  Follower 2  │       │
│  │              │                    │  Applier──►DB│       │
│  └──────┬───────┘                    └──────────────┘       │
│         │                                                   │
│         │ Raft (leader election only)                       │
│         └──────── heartbeats ──────── all nodes             │
│                                                             │
│  Reads  → any node (local)                                  │
│  Writes → leader (forwarded if received by follower)        │
└─────────────────────────────────────────────────────────────┘
```

---

# Key Insight: Raft vs WAL Separation

**Raft is used ONLY for leader election** — not for data replication.

| Concern | Mechanism |
|---|---|
| Leader election | hashicorp/raft (heartbeat, voting) |
| Data replication | WAL + gRPC log shipping |
| Follower replay | Applier (deterministic, sequential) |
| Write routing | HTTP forwarding to leader |

**Why?**
- Raft log replication has high overhead for graph mutations
- WAL is already the source of truth for durability
- Decoupled design: simpler, more efficient, easier to debug

---

# Leader Election — `replication/election.go`

```go
type ElectionConfig struct {
    NodeID     string
    BindAddr   string   // Raft protocol address
    Bootstrap  bool     // first node bootstraps
    Peers      []Peer
    OnRoleChange func(isLeader bool)
}
```

- **hashicorp/raft** with **raft-boltdb** for log & stable store
- Timeouts: Heartbeat **1s**, Election **1s**, LeaderLease **500ms**
- Observer loop watches `raft.LeaderObservation`
- On role change → callback triggers `db.SetRole("leader"|"follower")`
- Raft FSM is **effectively empty** — no real state machine

---

# WAL — Write-Ahead Log

```
WAL Frame:
┌──────────┬──────────────────┬──────────┐
│ 4B length│ msgpack WALEntry │ 4B CRC32 │
└──────────┴──────────────────┴──────────┘

WAL Entry:
{
    LSN:       uint64,          // monotonic sequence number
    Op:        OpType,          // one of 18 operation types
    NodeID:    uint64,          // leader-assigned ID
    Props:     map[string]any,  // node/edge properties
    ...
}
```

**18 operation types**: AddNode, AddNodeBatch, UpdateNode, SetNodeProps, DeleteNode, AddEdge, AddEdgeBatch, DeleteEdge, UpdateEdge, AddNodeWithLabels, AddLabel, RemoveLabel, CreateIndex, DropIndex, CreateCompositeIndex, DropCompositeIndex, CreateUniqueConstraint, DropUniqueConstraint

---

# WAL — Segments & Group Commit

```
data/
├── wal-0000000000.log    ← 64 MB segment
├── wal-0000000001.log    ← rotated when full
└── wal-0000000002.log    ← active segment
```

**Group Commit** — background goroutine batches `fsync` calls:

```
Without group commit:          With group commit (2ms batch):
  Write → fsync → Write        Write ──┐
  Write → fsync → Write        Write ──┤── batch fsync ──► disk
  Write → fsync → Write        Write ──┘
  ~80 ops/s (fsync bound)      ~10,000+ ops/s
```

- Writes are **buffered immediately** to OS
- fsync'd in **groups every 2ms**
- Eliminates per-write serialization bottleneck

---

# Applier — Follower Replay

```go
// applier.go — replays WAL entries with leader-assigned IDs
```

Key properties:
- **Deterministic** — uses leader's node/edge IDs (not local counters)
- **Idempotent** — skips already-applied LSN
- **Sequential** — single goroutine, preserves ordering
- **Bypasses write guard** — followers reject public writes, Applier uses internal path

```
Leader:  Write → WAL → gRPC stream ──────► Follower: gRPC → Applier → DB
                                                              │
                                                    (skip if LSN ≤ lastApplied)
```

---

# Write Forwarding

When a **follower** receives a write:

```
Client ──► Follower                Leader
              │                       │
   1. DB rejects: ErrReadOnlyReplica  │
   2. Router serializes as JSON       │
   3. HTTP POST /api/write  ─────────►│
              │                       │ 4. Execute locally
              │                       │ 5. Return result
   6. Response ◄──────────────────────│
              │                       │
   7. WAL → gRPC → Applier (async)    │
```

> Clients can connect to **any node** — reads are local, writes transparently forwarded.

---

# Running a 3-Node Cluster

```bash
# Node 1 — bootstraps as leader
go run ./cmd/graphdb-ui/ -db ./data1 \
  -node-id node1 -raft-addr 127.0.0.1:9001 \
  -grpc-addr 127.0.0.1:9101 -http-addr 127.0.0.1:7474 \
  -bootstrap -peers "node1@...9001@...9101@...7474,
                     node2@...9002@...9102@...7475,
                     node3@...9003@...9103@...7476"

# Node 2 — follower (auto-discovers leader)
go run ./cmd/graphdb-ui/ -db ./data2 \
  -node-id node2 -raft-addr 127.0.0.1:9002 ...

# Node 3 — follower
go run ./cmd/graphdb-ui/ -db ./data3 \
  -node-id node3 -raft-addr 127.0.0.1:9003 ...
```

Peer format: `id@raft_addr@grpc_addr@http_addr`

---

<!-- _class: lead -->

# 6. Go SDK

---

# SDK Architecture

```
sdk/goraphdb/
├── client.go          # New(addr, opts...) → *Client
├── transport.go       # HTTP calls (GET, POST, PUT, DELETE)
├── client_options.go  # WithTimeout, WithRetries, ...
├── node.go            # CreateNode, GetNode, UpdateNode, DeleteNode
├── edge.go            # CreateEdge, GetEdge, ...
├── cypher.go          # Query, QueryStream
├── index.go           # CreateIndex, DropIndex
├── constraint.go      # CreateUniqueConstraint, ...
├── prepared.go        # Prepare, Execute
├── iterator.go        # RowIterator (streaming)
├── admin.go           # Health, Stats, ClusterStatus
├── types.go           # Node, Edge, Props, CypherResult
└── errors.go          # Typed errors
```

> Communicates over **HTTP/JSON** — works with any GoraphDB server.

---

# SDK — Client Initialization

```go
import "github.com/mstrYoda/goraphdb/sdk/goraphdb"

// Simple
client := goraphdb.New("http://localhost:7474")

// With options
client := goraphdb.New("http://localhost:7474",
    goraphdb.WithTimeout(10*time.Second),
    goraphdb.WithRetries(3),
)
```

**Typed API** — no raw JSON handling:

```go
type Node struct {
    ID     uint64   `json:"id"`
    Labels []string `json:"labels"`
    Props  Props    `json:"props"`
}

type Edge struct {
    ID    uint64 `json:"id"`
    From  uint64 `json:"from"`
    To    uint64 `json:"to"`
    Label string `json:"label"`
    Props Props  `json:"props"`
}
```

---

# SDK — Core Operations

```go
ctx := context.Background()

// ── Nodes ──
id, _ := client.CreateNode(ctx, goraphdb.Props{"name": "Alice", "age": 30})
node, _ := client.GetNode(ctx, id)
client.UpdateNode(ctx, id, goraphdb.Props{"age": 31})
client.DeleteNode(ctx, id)

// ── Edges ──
edgeID, _ := client.CreateEdge(ctx, alice, bob, "FOLLOWS",
    goraphdb.Props{"since": "2024"})

// ── Cypher ──
result, _ := client.Query(ctx,
    `MATCH (a)-[:FOLLOWS]->(b) RETURN a.name, b.name`)

// ── Streaming ──
iter, _ := client.QueryStream(ctx, `MATCH (n) RETURN n.name`)
defer iter.Close()
for iter.Next() {
    fmt.Println(iter.Row()["n.name"])
}

// ── Admin ──
health, _ := client.Health(ctx)     // status, role
stats, _ := client.Stats(ctx)       // node/edge counts
cluster, _ := client.Cluster(ctx)   // topology
```

---

<!-- _class: lead -->

# 7. Code Examples

---

# Quick Start — Embedded Mode

```go
package main

import (
    "context"
    "fmt"
    graphdb "github.com/mstrYoda/goraphdb"
)

func main() {
    db, _ := graphdb.Open("./my.db", graphdb.DefaultOptions())
    defer db.Close()

    alice, _ := db.AddNode(graphdb.Props{"name": "Alice", "age": 30})
    bob, _   := db.AddNode(graphdb.Props{"name": "Bob", "age": 25})
    db.AddEdge(alice, bob, "follows", graphdb.Props{"since": "2024"})

    // Cypher query
    ctx := context.Background()
    res, _ := db.Cypher(ctx,
        `MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name`)

    for _, row := range res.Rows {
        fmt.Println(row["b.name"]) // Bob
    }
}
```

---

# Labels, Indexes & Constraints

```go
// Create nodes with labels
alice, _ := db.AddNodeWithLabels(
    []string{"Person", "Employee"},
    graphdb.Props{"name": "Alice", "email": "alice@co.com"},
)

// Unique constraint — no two :Person share the same email
db.CreateUniqueConstraint("Person", "email")

// Secondary index for fast lookups
db.CreateIndex("name")

// Composite index for compound queries
db.CreateCompositeIndex("city", "age")

// Index-backed Cypher — O(log N) instead of O(N)
res, _ := db.Cypher(ctx,
    `MATCH (n:Person {city: "Istanbul", age: 30}) RETURN n`)

// O(1) unique lookup
node, _ := db.FindByUniqueConstraint("Person", "email", "alice@co.com")
```

---

# Cypher — Read Patterns

```go
ctx := context.Background()

// All nodes
db.Cypher(ctx, `MATCH (n) RETURN n`)

// Property filter (uses index)
db.Cypher(ctx, `MATCH (n {name: "Alice"}) RETURN n`)

// WHERE clause
db.Cypher(ctx, `MATCH (n) WHERE n.age > 25 RETURN n`)

// 1-hop traversal
db.Cypher(ctx, `MATCH (a)-[:follows]->(b) RETURN a, b`)

// Variable-length path (1-3 hops)
db.Cypher(ctx, `MATCH (a)-[:follows*1..3]->(b) RETURN b`)

// OPTIONAL MATCH (left outer join)
db.Cypher(ctx, `MATCH (n:Person)
                 OPTIONAL MATCH (n)-[:WROTE]->(b)
                 RETURN n.name, b`)

// ORDER BY + LIMIT (top-K heap)
db.Cypher(ctx, `MATCH (n) RETURN n ORDER BY n.age DESC LIMIT 5`)
```

---

# Cypher — Write Patterns

```go
ctx := context.Background()

// CREATE nodes and edges
db.Cypher(ctx, `CREATE (a:Person {name: "Alice"})
                -[:FOLLOWS]->
                (b:Person {name: "Bob"})`)

// MERGE — upsert (match-or-create)
db.Cypher(ctx, `MERGE (n:Person {name: "Alice"})
                ON CREATE SET n.created = "2026"
                ON MATCH SET n.updated = "2026"`)

// SET — update properties
db.Cypher(ctx, `MATCH (n {name: "Alice"})
                SET n.age = 31
                RETURN n`)

// DELETE
db.Cypher(ctx, `MATCH (n {name: "old-node"}) DELETE n`)

// Parameterized queries ($param)
db.CypherWithParams(ctx,
    `MATCH (n {name: $name}) RETURN n`,
    map[string]any{"name": "Alice"})
```

---

# Graph Algorithms

```go
// BFS — breadth-first traversal
results, _ := db.BFSCollect(startID, 3, graphdb.Outgoing)

// DFS — depth-first traversal
results, _ := db.DFSCollect(startID, 3, graphdb.Outgoing)

// Shortest path (unweighted)
path, _ := db.ShortestPath(from, to)

// Dijkstra (weighted)
path, _ := db.ShortestPathWeighted(from, to, "weight", 1.0)

// All paths up to depth 5
paths, _ := db.AllPaths(from, to, 5)

// Connected components
components, _ := db.ConnectedComponents()

// Topological sort (Kahn's algorithm)
sorted, _ := db.TopologicalSort()

// Fluent query builder
result, _ := db.NewQuery().From(alice).FollowEdge("follows").
    Dir(graphdb.Outgoing).Depth(3).
    Where(func(n *graphdb.Node) bool { return n.GetFloat("age") > 25 }).
    Limit(10).UseBFS().Execute()
```

---

# Transactions

```go
// Multi-statement atomic operations
tx, _ := db.Begin()

alice, _ := tx.AddNode(graphdb.Props{"name": "Alice"})
bob, _   := tx.AddNode(graphdb.Props{"name": "Bob"})
tx.AddEdge(alice, bob, "follows", nil)

// Read uncommitted data within the same transaction
node, _ := tx.GetNode(alice) // visible before commit

err := tx.Commit()   // atomically persists all changes
// — or —
err = tx.Rollback()  // discards everything
```

**Semantics:** read-your-writes within the transaction, snapshot isolation for concurrent readers.

---

# Prepared Statements & Streaming

```go
// Parse once, execute many — no re-parsing overhead
pq, _ := db.PrepareCypher(
    "MATCH (n {name: $name}) RETURN n")

result, _ := db.ExecutePreparedWithParams(ctx, pq,
    map[string]any{"name": "Alice"})
result, _ = db.ExecutePreparedWithParams(ctx, pq,
    map[string]any{"name": "Bob"})

// Streaming — O(1) memory for large result sets
iter, _ := db.CypherStream(ctx,
    "MATCH (n) RETURN n.name LIMIT 1000000")
defer iter.Close()

for iter.Next() {
    row := iter.Row()
    fmt.Println(row["n.name"])
}
```

NDJSON streaming also available via `POST /api/cypher/stream`.

---

<!-- _class: lead -->

# 8. Other Approaches & What's Next

---

# What Else Is In There

| Feature | Details |
|---|---|
| **Management UI** | React + cytoscape.js graph viz, CodeMirror editor |
| **Prometheus Metrics** | 11 counters + 6 gauges, zero-dependency |
| **Slow Query Log** | Configurable threshold (default 100ms) |
| **Bloom Filter** | In-memory for `HasEdge()`, ~1 byte/edge |
| **Batch Operations** | `AddNodeBatch` / `AddEdgeBatch` — single fsync |
| **Cursor Pagination** | O(limit) per page, no offset scanning |
| **Worker Pool** | Goroutine pool for concurrent execution |
| **K8s Operator** | CRD, StatefulSet, health checks, Grafana dashboard |
| **Load Test CLI** | `graphdb-bench` with histogram reporting |
| **Graceful Shutdown** | HTTP drain → Raft stop → WAL flush → bbolt close |

---

# Benchmarks — Apple M-Series

<div class="columns">
<div>

| Operation | Time |
|---|---|
| AddNodeBatch (100K) | ~120 ms |
| CreateIndex (100K) | ~180 ms |
| FindByProperty (indexed) | < 1 ms |
| Cypher filter (indexed) | < 1 ms |
| Cypher 1-hop (indexed) | < 1 ms |
| ORDER BY+LIMIT (100K) | ~60 ms |
| 1000x cached Cypher | ~200 ms |

</div>
<div>

```bash
# Run benchmarks
go test -bench=. -benchmem

# Load test a cluster
go run ./cmd/graphdb-bench/ \
  -targets "http://127.0.0.1:7474" \
  -duration 30s \
  -writers 8 \
  -readers 16
```

</div>
</div>

---

# Kubernetes Operator

GoraphDB ships a **custom Kubernetes operator** that manages the full lifecycle of a cluster.

```
  kubectl apply -f goraphdb-cluster.yaml

  GoraphDBCluster CR
         │
         ▼
  ┌─────────────────────────────────────────────────────────┐
  │            GoraphDBCluster Reconciler                    │
  │                                                         │
  │  ① ConfigMap    (entrypoint script, peer topology)      │
  │  ② StatefulSet  (pods, PVCs, health probes)             │
  │  ③ Services     (headless, client, read)                │
  │  ④ PDB          (min quorum availability)               │
  │  ⑤ Status       (phase, leader, ready replicas)         │
  │  ⑥ Leader label (goraphdb.io/role=leader on pod)        │
  └─────────────────────────────────────────────────────────┘
```

---

# K8s Operator — Custom Resource Definition

```yaml
apiVersion: goraphdb.io/v1alpha1
kind: GoraphDBCluster
metadata:
  name: my-graph
spec:
  replicas: 3                    # 1, 3, or 5 for Raft quorum
  image: ghcr.io/mstryoda/goraphdb:v0.1.0

  shardCount: 4                  # hash-based sharding
  cacheBudget: 128Mi             # LRU node cache
  mmapSize: 256Mi                # bbolt mmap size
  slowQueryThreshold: 100ms

  storage:
    dataSize: 10Gi               # PVC for shard data
    walSize: 5Gi                 # PVC for WAL segments
    storageClassName: gp3        # cloud provider SSD

  ports:
    http: 7474
    raft: 7000
    grpc: 7001

  resources:
    requests: { cpu: "500m", memory: "512Mi" }
    limits:   { cpu: "2",    memory: "2Gi"   }
```

---

# K8s Operator — What Gets Created

<div class="columns">
<div>

**StatefulSet**
- Ordered pod startup (pod-0 first)
- PVCs: `data`, `wal`, `raft`
- Liveness/readiness: `GET /api/health`
- Entrypoint builds peer list from DNS

**ConfigMap**
- `entrypoint.sh` — extracts pod ordinal, builds `-peers`, `-node-id`, starts with `-bootstrap`

</div>
<div>

**Services**

| Service | Routes to |
|---|---|
| `my-graph-headless` | All pods (DNS discovery) |
| `my-graph-client` | Leader only (`role=leader`) |
| `my-graph-read` | All pods (read scaling) |

**PodDisruptionBudget**
- `minAvailable` = Raft quorum
- Prevents draining below majority

</div>
</div>

---

# K8s Operator — Bootstrap

**Bootstrap** (first deploy):

```
  pod-0 starts ──► Raft bootstrap ──► becomes leader
  pod-1 starts ──► joins cluster  ──► becomes follower
  pod-2 starts ──► joins cluster  ──► becomes follower

  Phase: Creating → Bootstrapping → Running
```

---

# K8s Operator — Failover

**Failover** (leader pod dies):

```
  pod-0 (leader) ──► KILLED
       │
       │  Raft election (~2-3s)
       ▼
  pod-1 ──► wins election ──► becomes leader
       │
       │  Operator reconcile (~30s)
       ▼
  Label update: pod-1 gets goraphdb.io/role=leader
       │
       ▼
  Client Service selector ──► routes to pod-1
```

> PVC persists Raft state — pods rejoin after restart without re-bootstrap.

---

# K8s Operator — Monitoring

<div class="columns">
<div>

**ServiceMonitor** (Prometheus Operator):

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: goraphdb
spec:
  selector:
    matchLabels:
      component: headless
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
```

</div>
<div>

**Grafana Dashboard** includes:
- Query rate & slow queries
- Node/edge counts (live)
- Cache hit rate & memory usage
- Write ops throughput
- Index & bloom filter usage
- Query cache hit ratio

All from GoraphDB's `GET /metrics` Prometheus endpoint.

</div>
</div>

---

# Alternative Approaches Considered

| Area | GoraphDB Chose | Alternatives |
|---|---|---|
| **Storage** | bbolt (B+tree, mmap) | LSM (RocksDB, Pebble), custom B+tree |
| **Encoding** | MessagePack + CRC32 | Protobuf, FlatBuffers, Cap'n Proto |
| **Query Language** | Cypher subset | Gremlin, SPARQL, GraphQL |
| **Replication** | WAL + gRPC streaming | Raft log replication, Paxos |
| **Election** | hashicorp/raft | etcd embed, ZooKeeper, custom Bully |
| **Sharding** | Hash (nodeID % N) | Consistent hashing, METIS partitioning |
| **Cache** | LRU byte-budgeted | Clock, ARC, LFU |
| **Index** | B+tree prefix scan | Inverted index, skip list, hash index |

---

# Roadmap — What's Next

**Near term:**
- **Vector Search** — HNSW index in-memory, bbolt `vectors` bucket, `CALL db.vectorSearch(...)`
- **Range Indexes** — B+tree range scans for `WHERE n.age > 25` without full scan
- **Hot Backup / Restore** — bbolt `WriteTo`, zero downtime
- **Auth & TLS** — user/password + encrypted connections

**Medium term:**
- **Change Data Capture** — streaming API for graph mutations
- **Distributed Query Coordinator** — scatter-gather for cross-shard queries
- **Shard Rebalancing** — automatic migration on node join/leave

---

<!-- _class: lead -->

# Summary

---

# GoraphDB at a Glance

```
   ┌─────────────┐
   │   Cypher    │  Industry-standard graph query language
   │   Engine    │  Lexer → Parser → AST → Index-aware executor
   ├─────────────┤
   │   Graph DB  │  Nodes, edges, labels, indexes, constraints
   │   Core      │  BFS, DFS, Dijkstra, topological sort
   ├─────────────┤
   │ Replication │  Raft election + WAL gRPC log shipping
   │   Layer     │  Write forwarding, automatic failover
   ├─────────────┤
   │   bbolt     │  B+tree, mmap, MVCC, CRC32 integrity
   │   Storage   │  Hash sharding, MessagePack encoding
   └─────────────┘
```

**Single binary** · **Embeddable** · **50 GB+ ready** · **Kubernetes native**

---

<!-- _class: lead -->

# Thank You

**github.com/mstrYoda/goraphdb**

Built with Go, bbolt, hashicorp/raft, and gRPC
