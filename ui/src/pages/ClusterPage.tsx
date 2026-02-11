import { useState, useEffect, useCallback } from 'react'
import {
  Server,
  Crown,
  Users,
  Wifi,
  WifiOff,
  RefreshCw,
  HardDrive,
  CircleDot,
  GitBranch,
  Activity,
  Gauge,
  BookOpen,
  PenLine,
  AlertTriangle,
} from 'lucide-react'
import { api } from '../api/client'
import type { ClusterNodesResponse, ClusterNodeInfo } from '../types'

const REFRESH_INTERVAL = 5000 // 5 seconds

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`
}

function formatNumber(n: number | undefined): string {
  if (n === undefined || n === null) return '—'
  return n.toLocaleString()
}

function roleBadgeColor(role: string): string {
  switch (role) {
    case 'leader':
      return 'bg-amber-500/15 text-amber-400 border-amber-500/30'
    case 'follower':
      return 'bg-blue-500/15 text-blue-400 border-blue-500/30'
    case 'standalone':
      return 'bg-purple-500/15 text-purple-400 border-purple-500/30'
    default:
      return 'bg-slate-500/15 text-slate-400 border-slate-500/30'
  }
}

function statusDotColor(status: string, reachable: boolean): string {
  if (!reachable) return 'bg-red-400'
  switch (status) {
    case 'ok':
      return 'bg-emerald-400'
    case 'readonly':
      return 'bg-amber-400'
    case 'unavailable':
      return 'bg-red-400'
    default:
      return 'bg-slate-400'
  }
}

function statusLabel(status: string, reachable: boolean): string {
  if (!reachable) return 'Unreachable'
  switch (status) {
    case 'ok':
      return 'Healthy'
    case 'readonly':
      return 'Read Only'
    case 'unavailable':
      return 'Unavailable'
    default:
      return status
  }
}

// ---------------------------------------------------------------------------
// Node Card
// ---------------------------------------------------------------------------

function NodeCard({ node, leaderWalLSN }: { node: ClusterNodeInfo; leaderWalLSN: number }) {
  const isLeader = node.role === 'leader'
  const RoleIcon = isLeader ? Crown : Users

  // Replication lag calculation
  const appliedLSN = node.replication?.applied_lsn ?? 0
  const walLSN = node.replication?.wal_last_lsn ?? 0
  const effectiveLeaderLSN = isLeader ? walLSN : leaderWalLSN
  const lag = effectiveLeaderLSN > 0 ? effectiveLeaderLSN - appliedLSN : 0
  const replPercent =
    effectiveLeaderLSN > 0 ? Math.min(100, (appliedLSN / effectiveLeaderLSN) * 100) : (isLeader ? 100 : 0)

  return (
    <div
      className={`bg-slate-900 border rounded-xl p-5 transition-all ${
        !node.reachable
          ? 'border-red-500/30 opacity-70'
          : isLeader
            ? 'border-amber-500/30'
            : 'border-slate-800'
      }`}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2.5">
          <div
            className={`p-2 rounded-lg ${
              isLeader ? 'bg-amber-500/10 text-amber-400' : 'bg-blue-500/10 text-blue-400'
            }`}
          >
            <RoleIcon className="w-4 h-4" />
          </div>
          <div>
            <h3 className="text-sm font-semibold text-white">{node.node_id}</h3>
            <span
              className={`inline-flex items-center px-2 py-0.5 text-[10px] font-medium rounded-full border mt-0.5 ${roleBadgeColor(node.role)}`}
            >
              {node.role.toUpperCase()}
            </span>
          </div>
        </div>

        {/* Status dot */}
        <div className="flex items-center gap-1.5">
          <div className={`w-2 h-2 rounded-full ${statusDotColor(node.status, node.reachable)} animate-pulse`} />
          <span className="text-[11px] text-slate-500">{statusLabel(node.status, node.reachable)}</span>
        </div>
      </div>

      {/* Unreachable state */}
      {!node.reachable && (
        <div className="flex items-center gap-2 text-red-400/80 text-xs mb-3 bg-red-500/5 rounded-lg p-3">
          <WifiOff className="w-3.5 h-3.5 flex-shrink-0" />
          <span>{node.error || 'Connection failed'}</span>
        </div>
      )}

      {/* Stats grid */}
      {node.reachable && node.stats && (
        <div className="grid grid-cols-2 gap-3 mb-4">
          <div className="flex items-center gap-2">
            <CircleDot className="w-3.5 h-3.5 text-blue-400/60" />
            <div>
              <p className="text-[10px] text-slate-600 uppercase">Nodes</p>
              <p className="text-sm font-semibold text-white">
                {formatNumber(node.stats.node_count)}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <GitBranch className="w-3.5 h-3.5 text-emerald-400/60" />
            <div>
              <p className="text-[10px] text-slate-600 uppercase">Edges</p>
              <p className="text-sm font-semibold text-white">
                {formatNumber(node.stats.edge_count)}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <HardDrive className="w-3.5 h-3.5 text-amber-400/60" />
            <div>
              <p className="text-[10px] text-slate-600 uppercase">Disk</p>
              <p className="text-sm font-semibold text-white">
                {formatBytes(node.stats.disk_size_bytes ?? 0)}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Gauge className="w-3.5 h-3.5 text-purple-400/60" />
            <div>
              <p className="text-[10px] text-slate-600 uppercase">Queries</p>
              <p className="text-sm font-semibold text-white">
                {formatNumber(node.metrics?.queries_total)}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Capabilities */}
      {node.reachable && (
        <div className="flex items-center gap-2 mb-4">
          <div
            className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] font-medium ${
              node.readable ? 'bg-emerald-500/10 text-emerald-400' : 'bg-slate-800 text-slate-600'
            }`}
          >
            <BookOpen className="w-3 h-3" />
            Reads
          </div>
          <div
            className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] font-medium ${
              node.writable ? 'bg-emerald-500/10 text-emerald-400' : 'bg-slate-800 text-slate-600'
            }`}
          >
            <PenLine className="w-3 h-3" />
            Writes
          </div>
        </div>
      )}

      {/* Replication progress */}
      {node.reachable && node.replication && (
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <span className="text-[10px] text-slate-600 uppercase">
              {isLeader ? 'WAL Progress' : 'Replication'}
            </span>
            <span className="text-[10px] text-slate-500">
              {isLeader
                ? `LSN ${formatNumber(walLSN)}`
                : `${formatNumber(appliedLSN)} / ${formatNumber(effectiveLeaderLSN)}`}
            </span>
          </div>
          <div className="w-full h-2 bg-slate-800 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-500 ${
                replPercent >= 99
                  ? 'bg-emerald-500'
                  : replPercent >= 80
                    ? 'bg-blue-500'
                    : replPercent >= 50
                      ? 'bg-amber-500'
                      : 'bg-red-500'
              }`}
              style={{ width: `${replPercent}%` }}
            />
          </div>
          {!isLeader && lag > 0 && (
            <p className="text-[10px] text-amber-400 mt-1">
              {lag} {lag === 1 ? 'entry' : 'entries'} behind leader
            </p>
          )}
          {!isLeader && lag === 0 && effectiveLeaderLSN > 0 && (
            <p className="text-[10px] text-emerald-400 mt-1">Fully synced</p>
          )}
        </div>
      )}

      {/* Addresses footer */}
      {node.reachable && (
        <div className="mt-4 pt-3 border-t border-slate-800/60">
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-slate-600">
            {node.http_addr && <span>HTTP: {node.http_addr}</span>}
            {node.grpc_addr && <span>gRPC: {node.grpc_addr}</span>}
            {node.raft_addr && <span>Raft: {node.raft_addr}</span>}
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Replication Overview Table
// ---------------------------------------------------------------------------

function ReplicationTable({
  nodes,
  leaderWalLSN,
}: {
  nodes: ClusterNodeInfo[]
  leaderWalLSN: number
}) {
  const followers = nodes.filter((n) => n.role === 'follower')
  if (followers.length === 0) return null

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <div className="flex items-center gap-2 mb-4">
        <Activity className="w-4 h-4 text-slate-500" />
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
          Replication Status
        </h2>
        {leaderWalLSN > 0 && (
          <span className="ml-auto text-xs text-slate-600">
            Leader WAL LSN: {formatNumber(leaderWalLSN)}
          </span>
        )}
      </div>

      <div className="space-y-3">
        {followers.map((node) => {
          const applied = node.replication?.applied_lsn ?? 0
          const lag = leaderWalLSN > 0 ? leaderWalLSN - applied : 0
          const pct = leaderWalLSN > 0 ? Math.min(100, (applied / leaderWalLSN) * 100) : 0

          return (
            <div key={node.node_id}>
              <div className="flex items-center justify-between mb-1">
                <div className="flex items-center gap-2">
                  <div
                    className={`w-1.5 h-1.5 rounded-full ${
                      node.reachable ? 'bg-blue-400' : 'bg-red-400'
                    }`}
                  />
                  <span className="text-sm text-white font-medium">{node.node_id}</span>
                </div>
                <div className="text-xs text-slate-500">
                  {node.reachable ? (
                    <>
                      <span className="text-white font-mono">{formatNumber(applied)}</span>
                      <span className="mx-1">/</span>
                      <span className="font-mono">{formatNumber(leaderWalLSN)}</span>
                      <span className="mx-2">·</span>
                      {lag === 0 ? (
                        <span className="text-emerald-400">synced</span>
                      ) : (
                        <span className="text-amber-400">lag={lag}</span>
                      )}
                      <span className="mx-2">·</span>
                      <span>{pct.toFixed(1)}%</span>
                    </>
                  ) : (
                    <span className="text-red-400">unreachable</span>
                  )}
                </div>
              </div>
              <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full transition-all duration-500 ${
                    !node.reachable
                      ? 'bg-red-500/50'
                      : pct >= 99
                        ? 'bg-emerald-500'
                        : pct >= 80
                          ? 'bg-blue-500'
                          : 'bg-amber-500'
                  }`}
                  style={{ width: `${node.reachable ? pct : 0}%` }}
                />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Cluster Overview Summary
// ---------------------------------------------------------------------------

function ClusterSummary({ data }: { data: ClusterNodesResponse }) {
  const total = data.nodes.length
  const healthy = data.nodes.filter((n) => n.reachable && n.status === 'ok').length
  const readonly = data.nodes.filter((n) => n.reachable && n.status === 'readonly').length
  const unreachable = data.nodes.filter((n) => !n.reachable).length
  const leader = data.nodes.find((n) => n.role === 'leader')

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-3 mb-6">
      <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
        <p className="text-[10px] text-slate-600 uppercase">Mode</p>
        <p className="text-lg font-bold text-white capitalize">{data.mode}</p>
      </div>
      <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
        <p className="text-[10px] text-slate-600 uppercase">Total Nodes</p>
        <p className="text-lg font-bold text-white">{total}</p>
      </div>
      <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
        <p className="text-[10px] text-slate-600 uppercase">Healthy</p>
        <p className="text-lg font-bold text-emerald-400">{healthy}</p>
      </div>
      {readonly > 0 && (
        <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
          <p className="text-[10px] text-slate-600 uppercase">Read Only</p>
          <p className="text-lg font-bold text-amber-400">{readonly}</p>
        </div>
      )}
      {unreachable > 0 && (
        <div className="bg-slate-900 border border-red-500/20 rounded-lg p-3">
          <p className="text-[10px] text-slate-600 uppercase">Unreachable</p>
          <p className="text-lg font-bold text-red-400">{unreachable}</p>
        </div>
      )}
      <div className="bg-slate-900 border border-slate-800 rounded-lg p-3">
        <p className="text-[10px] text-slate-600 uppercase">Leader</p>
        <p className="text-lg font-bold text-amber-400">
          {leader ? leader.node_id : <span className="text-red-400">None</span>}
        </p>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

export default function ClusterPage() {
  const [data, setData] = useState<ClusterNodesResponse | null>(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  const fetchData = useCallback(async (isManual = false) => {
    try {
      if (isManual) setRefreshing(true)
      const res = await api.getClusterNodes()
      setData(res)
      setError('')
      setLastUpdated(new Date())
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [])

  // Initial load + auto-refresh
  useEffect(() => {
    fetchData()
    const interval = setInterval(() => fetchData(), REFRESH_INTERVAL)
    return () => clearInterval(interval)
  }, [fetchData])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-slate-500">
        <RefreshCw className="w-5 h-5 animate-spin mr-2" />
        Loading cluster info…
      </div>
    )
  }

  if (error && !data) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 text-red-400 text-sm">
        <AlertTriangle className="w-4 h-4 inline mr-2" />
        Failed to load cluster info: {error}
      </div>
    )
  }

  if (!data) return null

  // Find leader's WAL LSN for replication lag calculation
  const leaderNode = data.nodes.find((n) => n.role === 'leader')
  const leaderWalLSN = leaderNode?.replication?.wal_last_lsn ?? 0

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-white">Cluster</h1>
          <p className="text-xs text-slate-500 mt-1">
            {data.mode === 'standalone' ? 'Running in standalone mode' : `Connected as ${data.self}`}
            {lastUpdated && (
              <span className="ml-2">
                · Updated {lastUpdated.toLocaleTimeString()}
              </span>
            )}
          </p>
        </div>
        <button
          onClick={() => fetchData(true)}
          disabled={refreshing}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-slate-400 bg-slate-800 rounded-lg hover:bg-slate-700 transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Error banner (non-fatal) */}
      {error && data && (
        <div className="bg-amber-500/10 border border-amber-500/20 rounded-lg p-3 text-amber-400 text-xs mb-4">
          <AlertTriangle className="w-3.5 h-3.5 inline mr-1" />
          Refresh failed: {error}. Showing last known state.
        </div>
      )}

      {/* Summary bar */}
      <ClusterSummary data={data} />

      {/* Node cards grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4 mb-6">
        {data.nodes.map((node) => (
          <NodeCard key={node.node_id} node={node} leaderWalLSN={leaderWalLSN} />
        ))}
      </div>

      {/* Replication overview */}
      {data.mode === 'cluster' && (
        <ReplicationTable nodes={data.nodes} leaderWalLSN={leaderWalLSN} />
      )}

      {/* Standalone hint */}
      {data.mode === 'standalone' && (
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 text-center">
          <Server className="w-8 h-8 text-slate-600 mx-auto mb-2" />
          <p className="text-sm text-slate-400">Running in Standalone Mode</p>
          <p className="text-xs text-slate-600 mt-1">
            Start with <code className="text-blue-400">-node-id</code> and{' '}
            <code className="text-blue-400">-peers</code> flags to enable cluster mode
          </p>
        </div>
      )}
    </div>
  )
}
