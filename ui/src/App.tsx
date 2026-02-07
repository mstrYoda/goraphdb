import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import QueryPage from './pages/QueryPage'
import IndexesPage from './pages/IndexesPage'
import ExplorerPage from './pages/ExplorerPage'
import MetricsPage from './pages/MetricsPage'
import SlowQueriesPage from './pages/SlowQueriesPage'

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Navigate to="/query" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/query" element={<QueryPage />} />
        <Route path="/metrics" element={<MetricsPage />} />
        <Route path="/slow-queries" element={<SlowQueriesPage />} />
        <Route path="/indexes" element={<IndexesPage />} />
        <Route path="/explorer" element={<ExplorerPage />} />
      </Routes>
    </Layout>
  )
}
