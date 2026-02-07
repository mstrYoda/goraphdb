import { useState, useEffect } from 'react'
import { AlertTriangle, Clock, Rows3, RefreshCw, Terminal } from 'lucide-react'
import { api } from '../api/client'
import type { SlowQueryEntry } from '../types'

function timeAgo(ts: string): string {
  const diff = Date.now() - new Date(ts).getTime()
  if (diff < 1000) return 'just now'
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
  return `${Math.floor(diff / 86_400_000)}d ago`
}

function durationBadge(ms: number) {
  const color =
    ms >= 1000
      ? 'bg-red-500/10 text-red-400 border-red-500/20'
      : ms >= 500
        ? 'bg-amber-500/10 text-amber-400 border-amber-500/20'
        : 'bg-yellow-500/10 text-yellow-400 border-yellow-500/20'
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 text-xs font-mono rounded-full border ${color}`}>
      <Clock className="w-3 h-3" />
      {ms.toFixed(1)} ms
    </span>
  )
}

export default function SlowQueriesPage() {
  const [queries, setQueries] = useState<SlowQueryEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [autoRefresh, setAutoRefresh] = useState(true)

  const fetchQueries = async () => {
    try {
      const res = await api.getSlowQueries(100)
      setQueries(res.queries ?? [])
      setError('')
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchQueries()
  }, [])

  useEffect(() => {
    if (!autoRefresh) return
    const id = setInterval(fetchQueries, 5000)
    return () => clearInterval(id)
  }, [autoRefresh])

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-white">Slow Queries</h1>
          <p className="text-xs text-slate-500 mt-1">
            Queries exceeding the configured threshold are captured here
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs transition-colors ${
              autoRefresh
                ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-400'
                : 'border-slate-700 bg-slate-800/50 text-slate-500'
            }`}
          >
            <RefreshCw className={`w-3.5 h-3.5 ${autoRefresh ? 'animate-spin' : ''}`} style={autoRefresh ? { animationDuration: '3s' } : {}} />
            {autoRefresh ? 'Live' : 'Paused'}
          </button>
          <button
            onClick={fetchQueries}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-500/10 border border-blue-500/20 text-blue-400 text-xs rounded-lg hover:bg-blue-500/20 transition-colors"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 text-red-400 text-sm mb-4">
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center h-64 text-slate-500">Loading…</div>
      ) : queries.length === 0 ? (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-12 text-center">
          <AlertTriangle className="w-10 h-10 text-slate-700 mx-auto mb-3" />
          <p className="text-slate-500 text-sm">No slow queries recorded yet</p>
          <p className="text-slate-600 text-xs mt-1">
            Queries exceeding the SlowQueryThreshold will appear here
          </p>
        </div>
      ) : (
        <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
          {/* Summary bar */}
          <div className="flex items-center gap-4 px-5 py-3 border-b border-slate-800 bg-slate-900/50">
            <div className="flex items-center gap-1.5 text-xs text-slate-400">
              <AlertTriangle className="w-3.5 h-3.5 text-amber-400" />
              <span className="font-semibold text-white">{queries.length}</span> slow queries
            </div>
            {queries.length > 0 && (
              <>
                <div className="w-px h-4 bg-slate-700" />
                <div className="flex items-center gap-1.5 text-xs text-slate-400">
                  <Clock className="w-3.5 h-3.5" />
                  Slowest: <span className="font-mono text-red-400">
                    {Math.max(...queries.map(q => q.duration_ms)).toFixed(1)} ms
                  </span>
                </div>
                <div className="w-px h-4 bg-slate-700" />
                <div className="flex items-center gap-1.5 text-xs text-slate-400">
                  <Clock className="w-3.5 h-3.5" />
                  Avg: <span className="font-mono text-amber-400">
                    {(queries.reduce((s, q) => s + q.duration_ms, 0) / queries.length).toFixed(1)} ms
                  </span>
                </div>
              </>
            )}
          </div>

          {/* Table */}
          <table className="w-full">
            <thead>
              <tr className="border-b border-slate-800">
                <th className="px-5 py-3 text-left text-[10px] text-slate-500 uppercase tracking-wider font-semibold">
                  Query
                </th>
                <th className="px-4 py-3 text-right text-[10px] text-slate-500 uppercase tracking-wider font-semibold w-28">
                  Duration
                </th>
                <th className="px-4 py-3 text-right text-[10px] text-slate-500 uppercase tracking-wider font-semibold w-20">
                  Rows
                </th>
                <th className="px-5 py-3 text-right text-[10px] text-slate-500 uppercase tracking-wider font-semibold w-28">
                  When
                </th>
              </tr>
            </thead>
            <tbody>
              {queries.map((q, i) => (
                <tr
                  key={i}
                  className="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors"
                >
                  <td className="px-5 py-3">
                    <div className="flex items-start gap-2">
                      <Terminal className="w-3.5 h-3.5 text-slate-600 mt-0.5 flex-shrink-0" />
                      <code className="text-xs text-slate-300 font-mono break-all leading-relaxed">
                        {q.query}
                      </code>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-right">
                    {durationBadge(q.duration_ms)}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <span className="inline-flex items-center gap-1 text-xs text-slate-400 font-mono">
                      <Rows3 className="w-3 h-3" />
                      {q.rows.toLocaleString()}
                    </span>
                  </td>
                  <td className="px-5 py-3 text-right text-xs text-slate-500">
                    {timeAgo(q.timestamp)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
