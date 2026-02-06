interface Props {
  columns: string[]
  rows: Record<string, any>[]
}

function formatValue(v: any): string {
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

export default function ResultsTable({ columns, rows }: Props) {
  if (columns.length === 0) return null

  return (
    <div className="overflow-auto border border-slate-800 rounded-lg">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-slate-900/80 sticky top-0">
            {columns.map((col) => (
              <th
                key={col}
                className="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider border-b border-slate-800"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              className="border-b border-slate-800/50 hover:bg-slate-900/50 transition-colors"
            >
              {columns.map((col) => (
                <td
                  key={col}
                  className="px-4 py-2 text-slate-300 font-mono text-xs max-w-xs truncate"
                  title={formatValue(row[col])}
                >
                  {formatValue(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
