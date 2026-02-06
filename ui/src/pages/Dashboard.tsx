import { useState, useEffect } from 'react'
import { CircleDot, GitBranch, HardDrive, Layers, Database } from 'lucide-react'
import StatsCard from '../components/StatsCard'
import { api } from '../api/client'
import type { GraphStats } from '../types'

export default function Dashboard() {
  const [stats, setStats] = useState<GraphStats | null>(null)
  const [indexes, setIndexes] = useState<string[]>([])
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([api.getStats(), api.listIndexes()])
      .then(([s, idx]) => {
        setStats(s)
        setIndexes(idx.indexes ?? [])
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-slate-500">
        Loading…
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 text-red-400 text-sm">
        Failed to load stats: {error}
      </div>
    )
  }

  const diskMB = stats ? (stats.disk_size_bytes / 1024 / 1024).toFixed(1) : '0'

  return (
    <div>
      <h1 className="text-2xl font-bold text-white mb-6">Dashboard</h1>

      {/* Stats cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-8">
        <StatsCard
          title="Nodes"
          value={stats?.node_count.toLocaleString() ?? '0'}
          icon={CircleDot}
          color="blue"
        />
        <StatsCard
          title="Edges"
          value={stats?.edge_count.toLocaleString() ?? '0'}
          icon={GitBranch}
          color="green"
        />
        <StatsCard
          title="Shards"
          value={stats?.shard_count ?? 0}
          icon={Layers}
          color="purple"
        />
        <StatsCard
          title="Disk Size"
          value={`${diskMB} MB`}
          icon={HardDrive}
          color="amber"
        />
      </div>

      {/* Active indexes */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div className="flex items-center gap-2 mb-4">
          <Database className="w-4 h-4 text-slate-500" />
          <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
            Active Indexes
          </h2>
        </div>

        {indexes.length === 0 ? (
          <p className="text-slate-600 text-sm">
            No indexes created yet. Go to{' '}
            <a href="/indexes" className="text-blue-400 hover:underline">
              Indexes
            </a>{' '}
            to create one.
          </p>
        ) : (
          <div className="flex flex-wrap gap-2">
            {indexes.map((idx) => (
              <span
                key={idx}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-blue-500/10 text-blue-400 text-xs font-mono rounded-full border border-blue-500/20"
              >
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                {idx}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Quick actions */}
      <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
        <a
          href="/query"
          className="bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-blue-500/30 transition-colors group"
        >
          <h3 className="text-sm font-semibold text-white group-hover:text-blue-400 transition-colors">
            Query Editor
          </h3>
          <p className="text-xs text-slate-500 mt-1">
            Run Cypher queries with live graph visualization
          </p>
        </a>
        <a
          href="/indexes"
          className="bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-blue-500/30 transition-colors group"
        >
          <h3 className="text-sm font-semibold text-white group-hover:text-blue-400 transition-colors">
            Index Management
          </h3>
          <p className="text-xs text-slate-500 mt-1">
            Create, drop, and rebuild property indexes
          </p>
        </a>
        <a
          href="/explorer"
          className="bg-slate-900 border border-slate-800 rounded-xl p-5 hover:border-blue-500/30 transition-colors group"
        >
          <h3 className="text-sm font-semibold text-white group-hover:text-blue-400 transition-colors">
            Graph Explorer
          </h3>
          <p className="text-xs text-slate-500 mt-1">
            Browse nodes, inspect properties, explore connections
          </p>
        </a>
      </div>
    </div>
  )
}
