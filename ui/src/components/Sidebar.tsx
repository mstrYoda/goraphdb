import { NavLink } from 'react-router-dom'
import { LayoutDashboard, Terminal, Database, Search, Activity } from 'lucide-react'

const links = [
  { to: '/query', icon: Terminal, label: 'Query' },
  { to: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/indexes', icon: Database, label: 'Indexes' },
  { to: '/explorer', icon: Search, label: 'Explorer' },
]

export default function Sidebar() {
  return (
    <aside className="w-56 bg-slate-900 border-r border-slate-800 flex flex-col flex-shrink-0">
      {/* Logo */}
      <div className="px-5 py-5 border-b border-slate-800">
        <div className="flex items-center gap-2.5">
          <Activity className="w-6 h-6 text-blue-400" />
          <span className="text-lg font-bold text-white tracking-tight">GraphDB</span>
        </div>
        <p className="text-[11px] text-slate-500 mt-1">Management Console</p>
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-4 space-y-0.5">
        {links.map((link) => (
          <NavLink
            key={link.to}
            to={link.to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-5 py-2.5 text-sm transition-colors ${
                isActive
                  ? 'text-blue-400 bg-blue-500/10 border-r-2 border-blue-400'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`
            }
          >
            <link.icon className="w-4 h-4" />
            {link.label}
          </NavLink>
        ))}
      </nav>

      {/* Footer */}
      <div className="px-5 py-4 border-t border-slate-800 text-[11px] text-slate-600">
        Powered by bbolt
      </div>
    </aside>
  )
}
