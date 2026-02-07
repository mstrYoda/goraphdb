import { useState, useEffect, useRef } from 'react'
import {
  Activity,
  Zap,
  AlertTriangle,
  XCircle,
  Clock,
  Database,
  CircleDot,
  GitBranch,
  Search,
  HardDrive,
  RefreshCw,
} from 'lucide-react'
import StatsCard from '../components/StatsCard'
import { api } from '../api/client'
import type { MetricsSnapshot } from '../types'

// Mini bar chart — renders a lightweight SVG bar chart from a ring buffer of values.
function MiniBarChart({
  data,
  color = '#3b82f6',
  height = 48,
}: {
  data: number[]
  color?: string
  height?: number
}) {
  const max = Math.max(...data, 1)
  const w = 100 / data.length
  return (
    <svg viewBox={`0 0 100 ${height}`} className="w-full" preserveAspectRatio="none">
      {data.map((v, i) => {
        const h = (v / max) * (height - 2)
        return (
          <rect
            key={i}
            x={i * w + w * 0.1}
            y={height - h}
            width={w * 0.8}
            height={h}
            fill={color}
            rx={1}
            opacity={0.8}
          />
        )
      })}
    </svg>
  )
}

// Gauge bar for cache usage, etc.
function GaugeBar({
  value,
  max,
  label,
  color = 'blue',
}: {
  value: number
  max: number
  label: string
  color?: string
}) {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0
  const colorMap: Record<string, string> = {
    blue: 'bg-blue-500',
    green: 'bg-emerald-500',
    amber: 'bg-amber-500',
    red: 'bg-red-500',
  }
  const barColor = pct > 90 ? colorMap.red : pct > 70 ? colorMap.amber : colorMap[color]
  return (
    <div>
      <div className="flex items-center justify-between text-xs mb-1.5">
        <span className="text-slate-400">{label}</span>
        <span className="text-slate-500 font-mono">{pct.toFixed(1)}%</span>
      </div>
      <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-500 ${barColor}`}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}

function formatDuration(us: number): string {
  if (us < 1000) return `${us} µs`
  if (us < 1000000) return `${(us / 1000).toFixed(1)} ms`
  return `${(us / 1000000).toFixed(2)} s`
}

const HISTORY_LEN = 30

export default function MetricsPage() {
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null)
  const [error, setError] = useState('')
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshInterval, setRefreshInterval] = useState(3)

  // History ring buffers for charts
  const queriesHistory = useRef<number[]>(new Array(HISTORY_LEN).fill(0))
  const errorsHistory = useRef<number[]>(new Array(HISTORY_LEN).fill(0))
  const slowHistory = useRef<number[]>(new Array(HISTORY_LEN).fill(0))
  const cacheHitHistory = useRef<number[]>(new Array(HISTORY_LEN).fill(0))
  const prevMetrics = useRef<MetricsSnapshot | null>(null)

  const fetchMetrics = async () => {
    try {
      const m = await api.getMetrics()
      setMetrics(m)
      setError('')

      // Compute deltas for charts
      const prev = prevMetrics.current
      if (prev) {
        const pushDelta = (arr: number[], cur: number, old: number) => {
          arr.shift()
          arr.push(Math.max(0, cur - old))
        }
        pushDelta(queriesHistory.current, m.queries_total, prev.queries_total)
        pushDelta(errorsHistory.current, m.query_errors_total, prev.query_errors_total)
        pushDelta(slowHistory.current, m.slow_queries_total, prev.slow_queries_total)
        pushDelta(cacheHitHistory.current, m.cache_hits_total, prev.cache_hits_total)
      }
      prevMetrics.current = m
    } catch (e: any) {
      setError(e.message)
    }
  }

  useEffect(() => {
    fetchMetrics()
  }, [])

  useEffect(() => {
    if (!autoRefresh) return
    const id = setInterval(fetchMetrics, refreshInterval * 1000)
    return () => clearInterval(id)
  }, [autoRefresh, refreshInterval])

  if (error && !metrics) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 text-red-400 text-sm">
        Failed to load metrics: {error}
      </div>
    )
  }

  const m = metrics
  const cacheTotal = (m?.cache_hits_total ?? 0) + (m?.cache_misses_total ?? 0)
  const cacheHitRate = cacheTotal > 0 ? ((m?.cache_hits_total ?? 0) / cacheTotal * 100) : 0

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-white">Metrics</h1>
        <div className="flex items-center gap-3">
          {/* Auto-refresh toggle */}
          <div className="flex items-center gap-2 text-xs text-slate-400">
            <button
              onClick={() => setAutoRefresh(!autoRefresh)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border transition-colors ${
                autoRefresh
                  ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-400'
                  : 'border-slate-700 bg-slate-800/50 text-slate-500'
              }`}
            >
              <RefreshCw className={`w-3.5 h-3.5 ${autoRefresh ? 'animate-spin' : ''}`} style={autoRefresh ? { animationDuration: '3s' } : {}} />
              {autoRefresh ? 'Live' : 'Paused'}
            </button>
            <select
              value={refreshInterval}
              onChange={(e) => setRefreshInterval(Number(e.target.value))}
              className="bg-slate-800 border border-slate-700 rounded-lg px-2 py-1.5 text-xs text-slate-300 focus:outline-none"
            >
              <option value={1}>1s</option>
              <option value={3}>3s</option>
              <option value={5}>5s</option>
              <option value={10}>10s</option>
            </select>
          </div>
          <button
            onClick={fetchMetrics}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-500/10 border border-blue-500/20 text-blue-400 text-xs rounded-lg hover:bg-blue-500/20 transition-colors"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Refresh
          </button>
        </div>
      </div>

      {/* Top stats row */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-6">
        <StatsCard
          title="Total Queries"
          value={m?.queries_total.toLocaleString() ?? '0'}
          subtitle={`${m?.slow_queries_total ?? 0} slow`}
          icon={Zap}
          color="blue"
        />
        <StatsCard
          title="Query Errors"
          value={m?.query_errors_total.toLocaleString() ?? '0'}
          icon={XCircle}
          color="amber"
        />
        <StatsCard
          title="Cache Hit Rate"
          value={`${cacheHitRate.toFixed(1)}%`}
          subtitle={`${(m?.cache_hits_total ?? 0).toLocaleString()} hits / ${(m?.cache_misses_total ?? 0).toLocaleString()} misses`}
          icon={Database}
          color="green"
        />
        <StatsCard
          title="Max Query Duration"
          value={formatDuration(m?.query_duration_max_us ?? 0)}
          subtitle={`Σ ${formatDuration(m?.query_duration_sum_us ?? 0)}`}
          icon={Clock}
          color="purple"
        />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        {/* Queries per interval */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-3">
            <Zap className="w-4 h-4 text-blue-400" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Queries / Interval
            </h2>
          </div>
          <div className="h-16">
            <MiniBarChart data={[...queriesHistory.current]} color="#3b82f6" height={64} />
          </div>
        </div>

        {/* Cache hits per interval */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-3">
            <Database className="w-4 h-4 text-emerald-400" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Cache Hits / Interval
            </h2>
          </div>
          <div className="h-16">
            <MiniBarChart data={[...cacheHitHistory.current]} color="#10b981" height={64} />
          </div>
        </div>

        {/* Slow queries per interval */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-3">
            <AlertTriangle className="w-4 h-4 text-amber-400" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Slow Queries / Interval
            </h2>
          </div>
          <div className="h-16">
            <MiniBarChart data={[...slowHistory.current]} color="#f59e0b" height={64} />
          </div>
        </div>

        {/* Errors per interval */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-3">
            <XCircle className="w-4 h-4 text-red-400" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Errors / Interval
            </h2>
          </div>
          <div className="h-16">
            <MiniBarChart data={[...errorsHistory.current]} color="#ef4444" height={64} />
          </div>
        </div>
      </div>

      {/* Gauges row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        {/* Node cache usage */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4">
            <HardDrive className="w-4 h-4 text-slate-500" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Node Cache
            </h2>
          </div>
          <GaugeBar
            value={m?.node_cache_bytes_used ?? 0}
            max={m?.node_cache_budget_bytes ?? 1}
            label={`${formatBytes(m?.node_cache_bytes_used ?? 0)} / ${formatBytes(m?.node_cache_budget_bytes ?? 0)}`}
            color="blue"
          />
        </div>

        {/* Query cache usage */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4">
            <Database className="w-4 h-4 text-slate-500" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Query Cache
            </h2>
          </div>
          <GaugeBar
            value={m?.query_cache_entries ?? 0}
            max={m?.query_cache_capacity ?? 1}
            label={`${(m?.query_cache_entries ?? 0).toLocaleString()} / ${(m?.query_cache_capacity ?? 0).toLocaleString()} entries`}
            color="green"
          />
        </div>
      </div>

      {/* Detailed counters */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Write operations */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4">
            <Activity className="w-4 h-4 text-slate-500" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Write Operations
            </h2>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <CounterTile label="Nodes Created" value={m?.nodes_created_total ?? 0} icon={CircleDot} color="text-emerald-400" />
            <CounterTile label="Nodes Deleted" value={m?.nodes_deleted_total ?? 0} icon={CircleDot} color="text-red-400" />
            <CounterTile label="Edges Created" value={m?.edges_created_total ?? 0} icon={GitBranch} color="text-emerald-400" />
            <CounterTile label="Edges Deleted" value={m?.edges_deleted_total ?? 0} icon={GitBranch} color="text-red-400" />
          </div>
        </div>

        {/* Live gauges */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4">
            <Search className="w-4 h-4 text-slate-500" />
            <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              Live Gauges
            </h2>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <CounterTile label="Nodes" value={m?.node_count ?? 0} icon={CircleDot} color="text-blue-400" />
            <CounterTile label="Edges" value={m?.edge_count ?? 0} icon={GitBranch} color="text-blue-400" />
            <CounterTile label="Index Lookups" value={m?.index_lookups_total ?? 0} icon={Search} color="text-purple-400" />
            <CounterTile label="Slow Queries" value={m?.slow_queries_total ?? 0} icon={AlertTriangle} color="text-amber-400" />
          </div>
        </div>
      </div>
    </div>
  )
}

function CounterTile({
  label,
  value,
  icon: Icon,
  color,
}: {
  label: string
  value: number
  icon: any
  color: string
}) {
  return (
    <div className="bg-slate-800/50 rounded-lg px-3 py-2.5 flex items-center gap-2.5">
      <Icon className={`w-4 h-4 ${color} flex-shrink-0`} />
      <div className="min-w-0">
        <p className="text-xs text-slate-500 truncate">{label}</p>
        <p className="text-sm font-bold text-white font-mono">{value.toLocaleString()}</p>
      </div>
    </div>
  )
}
