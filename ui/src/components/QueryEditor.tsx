import CodeMirror from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { Play } from 'lucide-react'

interface Props {
  value: string
  onChange: (val: string) => void
  onExecute: () => void
  loading?: boolean
}

export default function QueryEditor({ value, onChange, onExecute, loading }: Props) {
  return (
    <div className="border border-slate-800 rounded-lg overflow-hidden">
      <div
        onKeyDown={(e) => {
          if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault()
            onExecute()
          }
        }}
      >
        <CodeMirror
          value={value}
          onChange={onChange}
          height="140px"
          theme="dark"
          extensions={[sql()]}
          basicSetup={{
            lineNumbers: true,
            foldGutter: false,
            highlightActiveLine: true,
          }}
        />
      </div>
      <div className="flex items-center justify-between bg-slate-900/80 px-3 py-2 border-t border-slate-800">
        <span className="text-[11px] text-slate-600">
          ⌘ Enter or Ctrl+Enter to execute
        </span>
        <button
          onClick={onExecute}
          disabled={loading}
          className="flex items-center gap-1.5 px-4 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-sm font-medium rounded-md transition-colors"
        >
          <Play className="w-3.5 h-3.5" />
          {loading ? 'Running…' : 'Execute'}
        </button>
      </div>
    </div>
  )
}
