import { useRef, useEffect, useCallback } from 'react'
import cytoscape from 'cytoscape'
import type { GraphVizNode, GraphVizEdge } from '../types'

interface Props {
  nodes: GraphVizNode[]
  edges: GraphVizEdge[]
  onNodeClick?: (nodeId: number) => void
}

export default function GraphViewer({ nodes, edges, onNodeClick }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const cyRef = useRef<cytoscape.Core | null>(null)

  const buildElements = useCallback((): cytoscape.ElementDefinition[] => {
    const els: cytoscape.ElementDefinition[] = []
    for (const n of nodes) {
      els.push({
        data: {
          id: String(n.id),
          label: n.label || `#${n.id}`,
        },
      })
    }
    for (const e of edges) {
      els.push({
        data: {
          id: `e${e.id}`,
          source: String(e.from),
          target: String(e.to),
          label: e.label,
        },
      })
    }
    return els
  }, [nodes, edges])

  useEffect(() => {
    if (!containerRef.current) return

    const elements = buildElements()

    if (cyRef.current) {
      cyRef.current.destroy()
    }

    if (elements.length === 0) {
      cyRef.current = null
      return
    }

    const cy = cytoscape({
      container: containerRef.current,
      elements,
      layout: {
        name: nodes.length > 80 ? 'grid' : 'cose',
        animate: true,
        animationDuration: 400,
        nodeRepulsion: () => 8000,
        idealEdgeLength: () => 120,
      } as any,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': '#3b82f6',
            label: 'data(label)',
            color: '#e2e8f0',
            'font-size': '11px',
            'text-valign': 'bottom',
            'text-margin-y': 8,
            width: 32,
            height: 32,
            'border-width': 2,
            'border-color': '#1e40af',
          },
        },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': '#475569',
            'target-arrow-color': '#64748b',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            label: 'data(label)',
            'font-size': '9px',
            color: '#94a3b8',
            'text-rotation': 'autorotate',
            'text-margin-y': -10,
          } as any,
        },
        {
          selector: 'node:selected',
          style: {
            'background-color': '#60a5fa',
            'border-color': '#3b82f6',
            'border-width': 3,
          },
        },
        {
          selector: 'node:active',
          style: {
            'overlay-opacity': 0.1,
          },
        },
      ],
    })

    cy.on('tap', 'node', (evt) => {
      const id = parseInt(evt.target.id(), 10)
      if (onNodeClick && !isNaN(id)) onNodeClick(id)
    })

    cyRef.current = cy

    return () => {
      cy.destroy()
      cyRef.current = null
    }
  }, [buildElements, onNodeClick, nodes.length])

  if (nodes.length === 0) {
    return (
      <div className="w-full h-full min-h-[400px] bg-slate-950 rounded-lg border border-slate-800 flex items-center justify-center text-slate-600 text-sm">
        No graph data to display
      </div>
    )
  }

  return (
    <div
      ref={containerRef}
      className="w-full h-full min-h-[400px] bg-slate-950 rounded-lg border border-slate-800"
    />
  )
}
