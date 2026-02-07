import type {
  GraphStats,
  CypherResponse,
  IndexListResponse,
  NodeListResponse,
  GNode,
  NeighborhoodResponse,
  MetricsSnapshot,
  SlowQueryResponse,
  NodeCursorPage,
  EdgeCursorPage,
} from '../types'

const BASE = '/api'

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + url, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.error || `HTTP ${res.status}`)
  }
  return res.json()
}

export const api = {
  // ── Stats ──────────────────────────────────────────────────────────
  getStats: () => fetchJSON<GraphStats>('/stats'),

  // ── Cypher ─────────────────────────────────────────────────────────
  executeCypher: (query: string) =>
    fetchJSON<CypherResponse>('/cypher', {
      method: 'POST',
      body: JSON.stringify({ query }),
    }),

  // ── Indexes ────────────────────────────────────────────────────────
  listIndexes: () => fetchJSON<IndexListResponse>('/indexes'),
  createIndex: (property: string) =>
    fetchJSON<{ status: string }>('/indexes', {
      method: 'POST',
      body: JSON.stringify({ property }),
    }),
  dropIndex: (name: string) =>
    fetchJSON<{ status: string }>(`/indexes/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    }),
  reIndex: (name: string) =>
    fetchJSON<{ status: string }>(`/indexes/${encodeURIComponent(name)}/reindex`, {
      method: 'POST',
    }),

  // ── Nodes ──────────────────────────────────────────────────────────
  listNodes: (limit = 50, offset = 0) =>
    fetchJSON<NodeListResponse>(`/nodes?limit=${limit}&offset=${offset}`),
  getNode: (id: number) => fetchJSON<GNode>(`/nodes/${id}`),
  getNeighborhood: (id: number) =>
    fetchJSON<NeighborhoodResponse>(`/nodes/${id}/neighborhood`),
  createNode: (props: Record<string, any>) =>
    fetchJSON<{ id: number }>('/nodes', {
      method: 'POST',
      body: JSON.stringify({ props }),
    }),
  deleteNode: (id: number) =>
    fetchJSON<{ status: string }>(`/nodes/${id}`, { method: 'DELETE' }),

  // ── Edges ──────────────────────────────────────────────────────────
  createEdge: (from: number, to: number, label: string, props?: Record<string, any>) =>
    fetchJSON<{ id: number }>('/edges', {
      method: 'POST',
      body: JSON.stringify({ from, to, label, props }),
    }),
  deleteEdge: (id: number) =>
    fetchJSON<{ status: string }>(`/edges/${id}`, { method: 'DELETE' }),

  // ── Metrics ─────────────────────────────────────────────────────────
  getMetrics: () => fetchJSON<MetricsSnapshot>('/metrics'),

  // ── Slow Queries ────────────────────────────────────────────────────
  getSlowQueries: (limit = 50) =>
    fetchJSON<SlowQueryResponse>(`/slow-queries?limit=${limit}`),

  // ── Cursor Pagination ───────────────────────────────────────────────
  listNodesCursor: (cursor = 0, limit = 30) =>
    fetchJSON<NodeCursorPage>(`/nodes/cursor?cursor=${cursor}&limit=${limit}`),
  listEdgesCursor: (cursor = 0, limit = 30) =>
    fetchJSON<EdgeCursorPage>(`/edges/cursor?cursor=${cursor}&limit=${limit}`),
}
