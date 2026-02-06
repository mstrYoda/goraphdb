package graphdb

import (
	"encoding/json"
	"fmt"
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
	ID    NodeID `json:"id"`
	Props Props  `json:"props,omitempty"`
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
	// CacheSize is the LRU cache capacity for hot nodes (number of nodes).
	CacheSize int
	// NoSync disables fsync after each commit for faster writes (risk of data loss on crash).
	NoSync bool
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool
	// MmapSize is the initial mmap size for the database file in bytes.
	// Larger values improve performance for large datasets. Default: 256MB.
	MmapSize int
}

// DefaultOptions returns sensible defaults for a ~50GB graph database.
func DefaultOptions() Options {
	return Options{
		ShardCount:     1,
		WorkerPoolSize: 8,
		CacheSize:      100_000,
		NoSync:         false,
		ReadOnly:       false,
		MmapSize:       256 * 1024 * 1024, // 256MB initial mmap
	}
}

// GraphStats holds database statistics.
type GraphStats struct {
	NodeCount  uint64 `json:"node_count"`
	EdgeCount  uint64 `json:"edge_count"`
	ShardCount int    `json:"shard_count"`
	DiskSizeBytes int64  `json:"disk_size_bytes"`
}
