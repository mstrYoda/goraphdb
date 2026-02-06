import { useState, useCallback } from 'react'
import { Clock, Table, Share2 } from 'lucide-react'
import QueryEditor from '../components/QueryEditor'
import ResultsTable from '../components/ResultsTable'
import GraphViewer from '../components/GraphViewer'
import { api } from '../api/client'
import type { CypherResponse } from '../types'

const EXAMPLES = [
  'MATCH (n) RETURN n LIMIT 25',
  'MATCH (n {name: "Alice"}) RETURN n',
  'MATCH (n) WHERE n.age > 25 RETURN n.name, n.age',
  'MATCH (a)-[:follows]->(b) RETURN a, b LIMIT 25',
  'MATCH (a {name: "Alice"})-[:follows]->(b) RETURN b.name',
  'MATCH (a)-[:follows*1..2]->(b) RETURN b LIMIT 25',
  'MATCH (a)-[r]->(b) RETURN type(r), a, b LIMIT 25',
  'MATCH (n) RETURN n.name, n.age ORDER BY n.age DESC LIMIT 10',
]

export default function QueryPage() {
  const [query, setQuery] = useState('MATCH (n) RETURN n LIMIT 25')
  const [result, setResult] = useState<CypherResponse | null>(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [view, setView] = useState<'table' | 'graph'>('table')
  const [history, setHistory] = useState<string[]>([])

  const execute = useCallback(async () => {
    const q = query.trim()
    if (!q) return
    setLoading(true)
    setError('')
    setResult(null)

    try {
      const res = await api.executeCypher(q)
      setResult(res)
      // Auto-switch to graph view if results have graph data.
      if (res.graph.nodes.length > 0) {
        setView('graph')
      } else {
        setView('table')
      }
      setHistory((prev) =>
        [q, ...prev.filter((h) => h !== q)].slice(0, 20),
      )
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [query])

  return (
    <div className="flex gap-5 h-[calc(100vh-48px)]">
      {/* Main content */}
      <div className="flex-1 flex flex-col min-w-0">
        <h1 className="text-2xl font-bold text-white mb-4">Query Editor</h1>

        <QueryEditor
          value={query}
          onChange={setQuery}
          onExecute={execute}
          loading={loading}
        />

        {/* Status bar */}
        {result && (
          <div className="flex items-center gap-4 mt-3 text-xs text-slate-500">
            <span className="flex items-center gap-1">
              <Clock className="w-3 h-3" />
              {result.execTimeMs.toFixed(2)} ms
            </span>
            <span>{result.rowCount} row(s)</span>
            {result.graph.nodes.length > 0 && (
              <span>
                {result.graph.nodes.length} node(s) · {result.graph.edges.length} edge(s)
              </span>
            )}
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="mt-3 bg-red-500/10 border border-red-500/20 rounded-lg px-4 py-2.5 text-red-400 text-sm">
            {error}
          </div>
        )}

        {/* View toggle */}
        {result && (
          <div className="flex items-center gap-1 mt-4 mb-3">
            <button
              onClick={() => setView('table')}
              className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                view === 'table'
                  ? 'bg-slate-800 text-white'
                  : 'text-slate-500 hover:text-slate-300'
              }`}
            >
              <Table className="w-3.5 h-3.5" />
              Table
            </button>
            <button
              onClick={() => setView('graph')}
              className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                view === 'graph'
                  ? 'bg-slate-800 text-white'
                  : 'text-slate-500 hover:text-slate-300'
              }`}
            >
              <Share2 className="w-3.5 h-3.5" />
              Graph
            </button>
          </div>
        )}

        {/* Results area */}
        <div className="flex-1 overflow-auto min-h-0 mt-1">
          {loading && (
            <div className="flex items-center justify-center h-32 text-slate-500 text-sm">
              Executing query…
            </div>
          )}
          {result && view === 'table' && (
            <ResultsTable columns={result.columns} rows={result.rows} />
          )}
          {result && view === 'graph' && (
            <GraphViewer nodes={result.graph.nodes} edges={result.graph.edges} />
          )}
        </div>
      </div>

      {/* Right panel: examples + history */}
      <div className="w-60 flex-shrink-0 flex flex-col gap-4 overflow-auto">
        {/* Examples */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
          <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">
            Example Queries
          </h3>
          <div className="space-y-1">
            {EXAMPLES.map((ex, i) => (
              <button
                key={i}
                onClick={() => setQuery(ex)}
                className="block w-full text-left px-2.5 py-1.5 text-[11px] font-mono text-slate-400 hover:text-blue-400 hover:bg-slate-800/50 rounded transition-colors truncate"
                title={ex}
              >
                {ex}
              </button>
            ))}
          </div>
        </div>

        {/* History */}
        {history.length > 0 && (
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">
              History
            </h3>
            <div className="space-y-1">
              {history.map((h, i) => (
                <button
                  key={i}
                  onClick={() => setQuery(h)}
                  className="block w-full text-left px-2.5 py-1.5 text-[11px] font-mono text-slate-400 hover:text-blue-400 hover:bg-slate-800/50 rounded transition-colors truncate"
                  title={h}
                >
                  {h}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
