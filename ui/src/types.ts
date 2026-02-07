// ---------------------------------------------------------------------------
// API response types — mirror the Go server JSON responses.
// ---------------------------------------------------------------------------

export interface GraphStats {
  node_count: number
  edge_count: number
  shard_count: number
  disk_size_bytes: number
}

export interface GNode {
  id: number
  props: Record<string, any>
}

export interface GEdge {
  id: number
  from: number
  to: number
  label: string
  props?: Record<string, any>
}

export interface CypherResponse {
  columns: string[]
  rows: Record<string, any>[]
  graph: {
    nodes: GraphVizNode[]
    edges: GraphVizEdge[]
  }
  rowCount: number
  execTimeMs: number
}

export interface GraphVizNode {
  id: number
  props: Record<string, any>
  label: string
}

export interface GraphVizEdge {
  id: number
  from: number
  to: number
  label: string
}

export interface IndexListResponse {
  indexes: string[]
}

export interface NodeListResponse {
  nodes: GNode[]
  total: number
  limit: number
  offset: number
}

export interface NeighborhoodResponse {
  center: GraphVizNode
  neighbors: GraphVizNode[]
  edges: GraphVizEdge[]
}

// Metrics snapshot from GET /api/metrics
export interface MetricsSnapshot {
  queries_total: number
  slow_queries_total: number
  query_errors_total: number
  query_duration_sum_us: number
  query_duration_max_us: number
  cache_hits_total: number
  cache_misses_total: number
  nodes_created_total: number
  nodes_deleted_total: number
  edges_created_total: number
  edges_deleted_total: number
  index_lookups_total: number
  node_cache_bytes_used?: number
  node_cache_budget_bytes?: number
  node_count?: number
  edge_count?: number
  query_cache_entries?: number
  query_cache_capacity?: number
}

// Slow query entry from GET /api/slow-queries
export interface SlowQueryEntry {
  query: string
  duration_ms: number
  rows: number
  timestamp: string
}

export interface SlowQueryResponse {
  queries: SlowQueryEntry[]
  count: number
}

// Cursor pagination responses
export interface NodeCursorPage {
  nodes: CursorNode[]
  next_cursor: number
  has_more: boolean
  limit: number
}

export interface CursorNode {
  id: number
  labels?: string[]
  props: Record<string, any>
}

export interface EdgeCursorPage {
  edges: CursorEdge[]
  next_cursor: number
  has_more: boolean
  limit: number
}

export interface CursorEdge {
  id: number
  from: number
  to: number
  label: string
  props?: Record<string, any>
}
