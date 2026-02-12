package graphdb

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// NodeID uniquely identifies a node in the graph.
type NodeID uint64

// EdgeID uniquely identifies an edge in the graph.
type EdgeID uint64

// Props holds arbitrary key-value properties for nodes and edges.
type Props map[string]interface{}

// Direction represents the direction of an edge traversal.
type Direction byte

const (
	// Outgoing represents edges going from a node.
	Outgoing Direction = 0x01
	// Incoming represents edges coming to a node.
	Incoming Direction = 0x02
	// Both represents edges in both directions.
	Both Direction = 0x03
)

// Node represents a vertex in the graph with arbitrary properties.
type Node struct {
	ID     NodeID   `json:"id"`
	Labels []string `json:"labels,omitempty"`
	Props  Props    `json:"props,omitempty"`
}

// Get returns a property value by key, with an existence flag.
func (n *Node) Get(key string) (interface{}, bool) {
	v, ok := n.Props[key]
	return v, ok
}

// GetString returns a string property or empty string.
func (n *Node) GetString(key string) string {
	if v, ok := n.Props[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// GetFloat returns a float64 property or 0.
func (n *Node) GetFloat(key string) float64 {
	if v, ok := n.Props[key]; ok {
		switch f := v.(type) {
		case float64:
			return f
		case int:
			return float64(f)
		case int64:
			return float64(f)
		case json.Number:
			val, _ := f.Float64()
			return val
		}
	}
	return 0
}

// Edge represents a directed, labeled relationship between two nodes.
// Example: a ---follows---> b, x ---consumes---> y
type Edge struct {
	ID    EdgeID `json:"id"`
	From  NodeID `json:"from"`
	To    NodeID `json:"to"`
	Label string `json:"label"`
	Props Props  `json:"props,omitempty"`
}

// String returns a human-readable representation of the edge.
func (e *Edge) String() string {
	return fmt.Sprintf("(%d)--%s-->(%d)", e.From, e.Label, e.To)
}

// TraversalResult holds a node discovered during BFS/DFS along with traversal metadata.
type TraversalResult struct {
	Node  *Node   `json:"node"`
	Depth int     `json:"depth"`
	Path  []*Edge `json:"path,omitempty"` // edges from start to this node
}

// PathResult represents a path between two nodes.
type PathResult struct {
	Nodes []*Node `json:"nodes"`
	Edges []*Edge `json:"edges"`
	Cost  float64 `json:"cost"` // total cost (1.0 per hop for unweighted)
}

// NodeFilter is a predicate function that filters nodes during traversal/queries.
type NodeFilter func(*Node) bool

// EdgeFilter is a predicate function that filters edges during traversal/queries.
type EdgeFilter func(*Edge) bool

// Visitor is called for each node during traversal. Return false to stop traversal.
type Visitor func(result *TraversalResult) bool

// Options configures the GraphDB instance.
type Options struct {
	// ShardCount is the number of shards. Use 1 for single-process mode (default).
	ShardCount int
	// WorkerPoolSize is the number of goroutines for concurrent query execution.
	WorkerPoolSize int
	// CacheBudget is the LRU cache memory budget in bytes for hot nodes.
	// Nodes are evicted LRU-first when the total estimated size exceeds this budget.
	// Default: 128MB.
	CacheBudget int64
	// NoSync disables fsync after each commit for faster writes (risk of data loss on crash).
	NoSync bool
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool
	// MmapSize is the initial mmap size for the database file in bytes.
	// Larger values improve performance for large datasets. Default: 256MB.
	MmapSize int
	// SlowQueryThreshold is the duration threshold for slow query logging.
	// Queries exceeding this duration are logged at WARN level.
	// Default: 100ms. Set to 0 to disable slow query logging.
	SlowQueryThreshold time.Duration
	// Logger is the structured logger for all database operations.
	// If nil, slog.Default() is used.
	Logger *slog.Logger

	// --- Query Governor ---

	// MaxResultRows is the maximum number of rows a single query can return.
	// Queries that exceed this limit return ErrResultTooLarge.
	// Default: 0 (unlimited). A reasonable production value is 100,000.
	MaxResultRows int
	// DefaultQueryTimeout is applied when the caller's context has no deadline.
	// Prevents runaway queries from consuming resources indefinitely.
	// Default: 0 (no default timeout — caller must set their own).
	DefaultQueryTimeout time.Duration

	// --- Write Backpressure ---

	// WriteQueueSize is the maximum number of concurrent write operations
	// allowed per shard. Additional writers block until a slot is available
	// or their context expires. Default: 64.
	WriteQueueSize int
	// WriteTimeout is the maximum time a write operation will wait for a
	// slot in the write queue before returning ErrWriteQueueFull.
	// Default: 5s. Set to 0 for unlimited waiting (block until slot available).
	WriteTimeout time.Duration

	// --- Replication ---

	// Role determines the node's behavior in a replication cluster.
	//   - "" or "standalone": no replication (default)
	//   - "leader": accepts writes, records WAL, ships to followers
	//   - "follower": read-only, applies WAL entries from leader
	// When Role is "follower", all direct write API calls (AddNode, AddEdge, etc.)
	// are rejected with ErrReadOnlyReplica. Only the internal applier can write.
	Role string

	// --- Write-Ahead Log (WAL) ---

	// EnableWAL enables the write-ahead log for replication support.
	// When enabled, all committed mutations are recorded to an append-only
	// log that followers can replay to stay in sync with the leader.
	// Default: false (standalone mode — no WAL overhead).
	EnableWAL bool
	// WALNoSync skips fsync after each WAL write. Faster but risks losing
	// the last entry on crash. Only use for testing or benchmarks.
	WALNoSync bool

	// --- Compaction ---

	// CompactionInterval controls how often background compaction runs.
	// BoltDB files grow after deletes but never shrink; compaction rewrites
	// the file to reclaim space. Default: 0 (disabled — call DB.Compact()
	// manually). A typical production value is 1h.
	CompactionInterval time.Duration
}

// DefaultOptions returns sensible defaults for a ~50GB graph database.
//
// NoSync defaults to true for high write throughput: per-transaction fdatasync
// is disabled, and a background goroutine syncs each shard every 200ms.
// This means at most 200ms of committed writes may be lost on an unclean
// shutdown (power failure, OOM-kill). In cluster mode, the WAL provides
// additional durability (fsync every 2ms). Set NoSync=false if you need
// per-transaction durability at the cost of ~20x lower write throughput.
func DefaultOptions() Options {
	return Options{
		ShardCount:         1,
		WorkerPoolSize:     8,
		CacheBudget:        128 * 1024 * 1024, // 128MB
		SlowQueryThreshold: 100 * time.Millisecond,
		NoSync:             true,
		ReadOnly:           false,
		MmapSize:           256 * 1024 * 1024, // 256MB initial mmap
	}
}

// GraphStats holds database statistics.
type GraphStats struct {
	NodeCount     uint64 `json:"node_count"`
	EdgeCount     uint64 `json:"edge_count"`
	ShardCount    int    `json:"shard_count"`
	DiskSizeBytes int64  `json:"disk_size_bytes"`
}
