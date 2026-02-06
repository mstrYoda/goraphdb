import { useState, useEffect, useCallback } from 'react'
import { Plus, Trash2, RefreshCw, Database } from 'lucide-react'
import { api } from '../api/client'

export default function IndexesPage() {
  const [indexes, setIndexes] = useState<string[]>([])
  const [newProp, setNewProp] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [actionMsg, setActionMsg] = useState('')
  const [busy, setBusy] = useState(false)

  const load = useCallback(async () => {
    try {
      const res = await api.listIndexes()
      setIndexes(res.indexes ?? [])
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

  const flash = (msg: string) => {
    setActionMsg(msg)
    setTimeout(() => setActionMsg(''), 3000)
  }

  const create = async () => {
    const prop = newProp.trim()
    if (!prop) return
    setError('')
    setBusy(true)
    try {
      await api.createIndex(prop)
      setNewProp('')
      flash(`Index on "${prop}" created`)
      await load()
    } catch (e: any) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  const drop = async (name: string) => {
    if (!confirm(`Drop index on "${name}"?`)) return
    setError('')
    setBusy(true)
    try {
      await api.dropIndex(name)
      flash(`Index on "${name}" dropped`)
      await load()
    } catch (e: any) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  const reindex = async (name: string) => {
    setError('')
    setBusy(true)
    try {
      await api.reIndex(name)
      flash(`Index on "${name}" rebuilt`)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div>
      <h1 className="text-2xl font-bold text-white mb-6">Index Management</h1>

      {/* Create index */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 mb-6">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">
          Create New Index
        </h2>
        <p className="text-xs text-slate-500 mb-3">
          Property indexes speed up lookups in Cypher queries and{' '}
          <code className="text-slate-400">FindByProperty</code> calls.
        </p>
        <div className="flex gap-3">
          <input
            value={newProp}
            onChange={(e) => setNewProp(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && create()}
            placeholder="Property name (e.g. name, age, email)"
            className="flex-1 bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500 transition-colors"
          />
          <button
            onClick={create}
            disabled={busy || !newProp.trim()}
            className="flex items-center gap-2 px-5 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-white text-sm font-medium rounded-lg transition-colors"
          >
            <Plus className="w-4 h-4" />
            Create
          </button>
        </div>
      </div>

      {/* Messages */}
      {error && (
        <div className="mb-4 bg-red-500/10 border border-red-500/20 rounded-lg px-4 py-2.5 text-red-400 text-sm">
          {error}
        </div>
      )}
      {actionMsg && (
        <div className="mb-4 bg-emerald-500/10 border border-emerald-500/20 rounded-lg px-4 py-2.5 text-emerald-400 text-sm">
          {actionMsg}
        </div>
      )}

      {/* Index list */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-4">
          Active Indexes ({indexes.length})
        </h2>

        {loading ? (
          <p className="text-slate-500 text-sm">Loading…</p>
        ) : indexes.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16 text-slate-600">
            <Database className="w-10 h-10 mb-3 opacity-50" />
            <p className="text-sm">No indexes created yet</p>
            <p className="text-xs mt-1 text-slate-700">
              Indexes dramatically speed up property lookups
            </p>
          </div>
        ) : (
          <div className="space-y-2">
            {indexes.map((idx) => (
              <div
                key={idx}
                className="flex items-center justify-between bg-slate-800/50 border border-slate-700/50 rounded-lg px-4 py-3"
              >
                <div className="flex items-center gap-3">
                  <div className="w-2 h-2 rounded-full bg-emerald-400 flex-shrink-0" />
                  <span className="font-mono text-sm text-white">{idx}</span>
                  <span className="text-[10px] text-slate-500 bg-slate-800 px-2 py-0.5 rounded">
                    B+TREE
                  </span>
                </div>
                <div className="flex items-center gap-1.5">
                  <button
                    onClick={() => reindex(idx)}
                    disabled={busy}
                    className="p-1.5 text-slate-500 hover:text-blue-400 disabled:opacity-30 transition-colors"
                    title="Rebuild index"
                  >
                    <RefreshCw className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => drop(idx)}
                    disabled={busy}
                    className="p-1.5 text-slate-500 hover:text-red-400 disabled:opacity-30 transition-colors"
                    title="Drop index"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
