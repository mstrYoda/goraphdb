import { useState, useEffect, useCallback } from 'react'
import { ChevronLeft, ChevronRight, Trash2 } from 'lucide-react'
import GraphViewer from '../components/GraphViewer'
import { api } from '../api/client'
import type { GNode, GraphVizNode, GraphVizEdge } from '../types'

const PAGE_SIZE = 30

export default function ExplorerPage() {
  const [nodes, setNodes] = useState<GNode[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const [loading, setLoading] = useState(true)

  // Selection state
  const [selectedNode, setSelectedNode] = useState<GNode | null>(null)
  const [graphNodes, setGraphNodes] = useState<GraphVizNode[]>([])
  const [graphEdges, setGraphEdges] = useState<GraphVizEdge[]>([])
  const [edgeCount, setEdgeCount] = useState(0)

  // Load node list
  const loadList = useCallback(async () => {
    setLoading(true)
    try {
      const res = await api.listNodes(PAGE_SIZE, offset)
      setNodes(res.nodes ?? [])
      setTotal(res.total)
    } catch (e: any) {
      console.error('Failed to load nodes:', e)
    } finally {
      setLoading(false)
    }
  }, [offset])

  useEffect(() => {
    loadList()
  }, [loadList])

  // Select and explore a node
  const selectNode = useCallback(async (node: GNode) => {
    setSelectedNode(node)
    try {
      const hood = await api.getNeighborhood(node.id)

      const vizNodes: GraphVizNode[] = [
        hood.center,
        ...hood.neighbors,
      ]
      setGraphNodes(vizNodes)
      setGraphEdges(hood.edges)
      setEdgeCount(hood.edges.length)
    } catch (e: any) {
      console.error('Failed to load neighborhood:', e)
    }
  }, [])

  // Explore a node from the graph (click-to-expand)
  const exploreById = useCallback(async (id: number) => {
    try {
      const n = await api.getNode(id)
      selectNode(n)
    } catch {
      // ignore — node might have been deleted
    }
  }, [selectNode])

  // Delete a node
  const deleteNode = async (id: number) => {
    if (!confirm(`Delete node ${id} and all its edges?`)) return
    try {
      await api.deleteNode(id)
      if (selectedNode?.id === id) {
        setSelectedNode(null)
        setGraphNodes([])
        setGraphEdges([])
      }
      await loadList()
    } catch (e: any) {
      console.error(e)
    }
  }

  const displayLabel = (n: GNode) =>
    n.props?.name ?? n.props?.title ?? n.props?.label ?? `Node ${n.id}`

  return (
    <div className="flex gap-5 h-[calc(100vh-48px)]">
      {/* Left: node list */}
      <div className="w-72 flex-shrink-0 flex flex-col">
        <h1 className="text-2xl font-bold text-white mb-4">Explorer</h1>

        <div className="flex-1 overflow-auto bg-slate-900 border border-slate-800 rounded-xl">
          {loading ? (
            <div className="flex items-center justify-center h-32 text-slate-500 text-sm">
              Loading…
            </div>
          ) : nodes.length === 0 ? (
            <div className="flex items-center justify-center h-32 text-slate-600 text-sm">
              No nodes found
            </div>
          ) : (
            <div className="divide-y divide-slate-800/50">
              {nodes.map((node) => (
                <div
                  key={node.id}
                  onClick={() => selectNode(node)}
                  className={`flex items-center justify-between px-4 py-3 cursor-pointer transition-colors ${
                    selectedNode?.id === node.id
                      ? 'bg-blue-500/10 border-l-2 border-blue-400'
                      : 'hover:bg-slate-800/50 border-l-2 border-transparent'
                  }`}
                >
                  <div className="min-w-0">
                    <p className="text-sm font-medium text-white truncate">
                      {displayLabel(node)}
                    </p>
                    <p className="text-[11px] text-slate-500 font-mono">
                      ID: {node.id}
                    </p>
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation()
                      deleteNode(node.id)
                    }}
                    className="p-1 text-slate-700 hover:text-red-400 flex-shrink-0 transition-colors"
                    title="Delete node"
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Pagination */}
        <div className="flex items-center justify-between mt-3 text-xs text-slate-500">
          <span>
            {total === 0
              ? 'empty'
              : `${offset + 1}–${Math.min(offset + PAGE_SIZE, total)} of ${total}`}
          </span>
          <div className="flex gap-1">
            <button
              onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              disabled={offset === 0}
              className="p-1 rounded hover:bg-slate-800 disabled:opacity-30 transition-colors"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            <button
              onClick={() => setOffset(offset + PAGE_SIZE)}
              disabled={offset + PAGE_SIZE >= total}
              className="p-1 rounded hover:bg-slate-800 disabled:opacity-30 transition-colors"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Right: detail + graph */}
      <div className="flex-1 flex flex-col min-w-0">
        {selectedNode ? (
          <>
            {/* Node properties card */}
            <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 mb-4 flex-shrink-0">
              <div className="flex items-center justify-between mb-3">
                <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
                  Node {selectedNode.id}
                </h2>
                <span className="text-xs text-slate-600">
                  {edgeCount} edge(s)
                </span>
              </div>
              <div className="grid grid-cols-2 lg:grid-cols-3 gap-2">
                {Object.entries(selectedNode.props ?? {}).map(([k, v]) => (
                  <div
                    key={k}
                    className="bg-slate-800/50 rounded-lg px-3 py-2"
                  >
                    <span className="text-[10px] text-slate-500 uppercase block">
                      {k}
                    </span>
                    <p className="text-sm text-white font-mono truncate" title={String(v)}>
                      {String(v)}
                    </p>
                  </div>
                ))}
              </div>
            </div>

            {/* Graph visualization */}
            <div className="flex-1 min-h-0">
              <GraphViewer
                nodes={graphNodes}
                edges={graphEdges}
                onNodeClick={exploreById}
              />
            </div>
          </>
        ) : (
          <div className="flex-1 flex items-center justify-center text-slate-600">
            <div className="text-center">
              <p className="text-sm">Select a node to explore</p>
              <p className="text-xs mt-1 text-slate-700">
                Click a node to see its properties and connections
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
