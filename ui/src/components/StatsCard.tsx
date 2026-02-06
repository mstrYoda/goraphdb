import type { LucideIcon } from 'lucide-react'

interface Props {
  title: string
  value: string | number
  subtitle?: string
  icon: LucideIcon
  color?: 'blue' | 'green' | 'purple' | 'amber'
}

const palette = {
  blue: 'text-blue-400 bg-blue-500/10',
  green: 'text-emerald-400 bg-emerald-500/10',
  purple: 'text-purple-400 bg-purple-500/10',
  amber: 'text-amber-400 bg-amber-500/10',
}

export default function StatsCard({
  title,
  value,
  subtitle,
  icon: Icon,
  color = 'blue',
}: Props) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs text-slate-500 uppercase tracking-wider font-medium">
            {title}
          </p>
          <p className="text-2xl font-bold text-white mt-1">{value}</p>
          {subtitle && <p className="text-xs text-slate-500 mt-1">{subtitle}</p>}
        </div>
        <div className={`p-2.5 rounded-lg ${palette[color]}`}>
          <Icon className="w-5 h-5" />
        </div>
      </div>
    </div>
  )
}
