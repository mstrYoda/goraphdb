package graphdb

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Metrics holds operational counters for the database.
// All fields are atomic — safe for concurrent reads/writes with zero contention.
// The struct is intentionally dependency-free; Prometheus exposition format is
// generated manually so the core library doesn't pull in prometheus/client_golang.
type Metrics struct {
	// Query counters
	QueriesTotal    atomic.Uint64 // all Cypher executions (Cypher + CypherStream + prepared)
	SlowQueries     atomic.Uint64 // queries exceeding SlowQueryThreshold
	QueryErrorTotal atomic.Uint64 // queries that returned an error

	// Query duration tracking (for histogram approximation)
	QueryDurationSum atomic.Int64 // cumulative microseconds
	QueryDurationMax atomic.Int64 // max observed microseconds

	// Cache counters (mirrors from query cache + node cache)
	CacheHits   atomic.Uint64
	CacheMisses atomic.Uint64

	// Node cache byte tracking
	NodeCacheBytesUsed   atomic.Int64
	NodeCacheBudgetBytes atomic.Int64

	// Node/Edge write counters
	NodesCreated atomic.Uint64
	NodesDeleted atomic.Uint64
	EdgesCreated atomic.Uint64
	EdgesDeleted atomic.Uint64

	// Index counters
	IndexLookups atomic.Uint64

	// Bloom filter counters
	BloomNegatives atomic.Uint64 // HasEdge() calls avoided by bloom filter (true negatives)

	// Internal reference to DB for pulling live gauges
	db *DB
}

// newMetrics creates a Metrics instance bound to the given DB.
func newMetrics(db *DB) *Metrics {
	m := &Metrics{db: db}
	m.NodeCacheBudgetBytes.Store(db.opts.CacheBudget)
	return m
}

// recordQueryDuration records a query's wall-clock duration.
func (m *Metrics) recordQueryDuration(d time.Duration) {
	us := d.Microseconds()
	m.QueryDurationSum.Add(us)
	// Update max (CAS loop)
	for {
		cur := m.QueryDurationMax.Load()
		if us <= cur {
			break
		}
		if m.QueryDurationMax.CompareAndSwap(cur, us) {
			break
		}
	}
}

// Snapshot returns a point-in-time copy of all metrics as a map.
func (m *Metrics) Snapshot() map[string]any {
	snap := map[string]any{
		"queries_total":       m.QueriesTotal.Load(),
		"slow_queries_total":  m.SlowQueries.Load(),
		"query_errors_total":  m.QueryErrorTotal.Load(),
		"query_duration_sum_us": m.QueryDurationSum.Load(),
		"query_duration_max_us": m.QueryDurationMax.Load(),
		"cache_hits_total":    m.CacheHits.Load(),
		"cache_misses_total":  m.CacheMisses.Load(),
		"nodes_created_total": m.NodesCreated.Load(),
		"nodes_deleted_total": m.NodesDeleted.Load(),
		"edges_created_total": m.EdgesCreated.Load(),
		"edges_deleted_total": m.EdgesDeleted.Load(),
		"index_lookups_total":     m.IndexLookups.Load(),
		"bloom_negatives_total":   m.BloomNegatives.Load(),
	}

	// Pull live gauges from DB.
	if m.db != nil {
		snap["node_cache_bytes_used"] = m.db.ncache.TotalBytes()
		snap["node_cache_budget_bytes"] = m.db.ncache.BudgetBytes()
		snap["node_count"] = m.db.NodeCount()
		snap["edge_count"] = m.db.EdgeCount()
		cs := m.db.cache.stats()
		snap["query_cache_entries"] = cs.Entries
		snap["query_cache_capacity"] = cs.Capacity

		if m.db.edgeBloom != nil {
			snap["bloom_filter_bits"] = m.db.edgeBloom.size
			snap["bloom_filter_memory_bytes"] = uint64(len(m.db.edgeBloom.bits)) * 8
		}
	}

	return snap
}

// WritePrometheus writes all metrics in Prometheus text exposition format.
func (m *Metrics) WritePrometheus(w io.Writer) {
	// Counters
	pCounter(w, "graphdb_queries_total", "Total number of Cypher query executions", m.QueriesTotal.Load())
	pCounter(w, "graphdb_slow_queries_total", "Total number of slow queries", m.SlowQueries.Load())
	pCounter(w, "graphdb_query_errors_total", "Total number of query errors", m.QueryErrorTotal.Load())
	pCounter(w, "graphdb_query_duration_microseconds_sum", "Cumulative query duration in microseconds", uint64(m.QueryDurationSum.Load()))
	pCounter(w, "graphdb_cache_hits_total", "Total query cache hits", m.CacheHits.Load())
	pCounter(w, "graphdb_cache_misses_total", "Total query cache misses", m.CacheMisses.Load())
	pCounter(w, "graphdb_nodes_created_total", "Total nodes created", m.NodesCreated.Load())
	pCounter(w, "graphdb_nodes_deleted_total", "Total nodes deleted", m.NodesDeleted.Load())
	pCounter(w, "graphdb_edges_created_total", "Total edges created", m.EdgesCreated.Load())
	pCounter(w, "graphdb_edges_deleted_total", "Total edges deleted", m.EdgesDeleted.Load())
	pCounter(w, "graphdb_index_lookups_total", "Total index lookups", m.IndexLookups.Load())
	pCounter(w, "graphdb_bloom_negatives_total", "HasEdge calls avoided by bloom filter", m.BloomNegatives.Load())

	// Gauges (live values from DB)
	if m.db != nil {
		pGauge(w, "graphdb_node_cache_bytes_used", "Current bytes used by node cache", float64(m.db.ncache.TotalBytes()))
		pGauge(w, "graphdb_node_cache_budget_bytes", "Node cache budget in bytes", float64(m.db.ncache.BudgetBytes()))
		pGauge(w, "graphdb_nodes_current", "Current number of nodes", float64(m.db.NodeCount()))
		pGauge(w, "graphdb_edges_current", "Current number of edges", float64(m.db.EdgeCount()))

		cs := m.db.cache.stats()
		pGauge(w, "graphdb_query_cache_entries", "Current query cache entries", float64(cs.Entries))
		pGauge(w, "graphdb_query_cache_capacity", "Query cache max capacity", float64(cs.Capacity))
	}

	pGauge(w, "graphdb_query_duration_microseconds_max", "Maximum observed query duration in microseconds", float64(m.QueryDurationMax.Load()))
}

// ---------------------------------------------------------------------------
// Prometheus text format helpers
// ---------------------------------------------------------------------------

func pCounter(w io.Writer, name, help string, val uint64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, val)
}

func pGauge(w io.Writer, name, help string, val float64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", name, help, name, name, val)
}
