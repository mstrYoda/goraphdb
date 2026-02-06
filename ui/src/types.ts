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
