import { useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Database, List } from 'lucide-react'
import { Card } from './components/ui/Card'
import { Badge } from './components/ui/Badge'
import { Tabs, TabsList, TabsTrigger, TabsContent } from './components/ui/Tabs'
import { IngestionForm } from './components/IngestionForm'
import { JobList } from './components/JobList'
import { useJobs } from './hooks/useJobs'
import { useWebSocket } from './hooks/useWebSocket'

const queryClient = new QueryClient()

function StatsGrid() {
  const { data } = useJobs()
  useWebSocket() // Connect to WebSocket for real-time updates

  const stats = {
    total: (data?.jobs || []).length,
    running: (data?.jobs || []).filter(j => j.status === 'running' || j.status === 'pending').length,
    completed: (data?.jobs || []).filter(j => j.status === 'completed').length,
    failed: (data?.jobs || []).filter(j => j.status === 'failed').length,
  }

  return (
    <div className="grid grid-cols-4 gap-3 mb-6">
      <div className="bg-gray-800/30 rounded-lg p-3 border border-gray-800 hover:border-gray-700 transition-colors">
        <p className="text-xs text-gray-500 mb-0.5">Total</p>
        <p className="text-xl font-semibold text-foreground">{stats.total}</p>
      </div>
      <div className="bg-gray-800/30 rounded-lg p-3 border border-gray-800 hover:border-gray-700 transition-colors">
        <p className="text-xs text-gray-500 mb-0.5">Running</p>
        <p className="text-xl font-semibold text-accent">{stats.running}</p>
      </div>
      <div className="bg-gray-800/30 rounded-lg p-3 border border-gray-800 hover:border-gray-700 transition-colors">
        <p className="text-xs text-gray-500 mb-0.5">Completed</p>
        <p className="text-xl font-semibold text-success">{stats.completed}</p>
      </div>
      <div className="bg-gray-800/30 rounded-lg p-3 border border-gray-800 hover:border-gray-700 transition-colors">
        <p className="text-xs text-gray-500 mb-0.5">Failed</p>
        <p className="text-xl font-semibold text-error">{stats.failed}</p>
      </div>
    </div>
  )
}

function AppContent() {
  const [activeTab, setActiveTab] = useState('ingestion')

  const handleIngestionSuccess = () => {
    setActiveTab('jobs')
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="max-w-6xl mx-auto px-6 py-8">
        {/* Header */}
        <header className="mb-8 animate-slide-up">
          <div className="relative backdrop-blur-xl bg-gradient-to-r from-gray-900/40 via-gray-800/30 to-gray-900/40 rounded-2xl border border-gray-800/50 p-6 overflow-hidden">
            {/* Glassmorphic overlay */}
            <div className="absolute inset-0 bg-gradient-to-br from-accent/5 via-transparent to-purple-500/5 pointer-events-none" />

            {/* Content */}
            <div className="relative flex items-center justify-between">
              <div>
                <h1 className="text-4xl font-bold tracking-tight bg-gradient-to-r from-white via-gray-100 to-gray-300 bg-clip-text text-transparent mb-2">
                  Chug
                </h1>
                <p className="text-sm text-gray-400">
                  PostgreSQL to ClickHouse ETL Pipeline
                </p>
              </div>

              <div className="flex items-center gap-2">
                <Badge pulse variant="success">
                  Connected
                </Badge>
              </div>
            </div>
          </div>
        </header>

        {/* Main Tabs */}
        <Tabs value={activeTab} onChange={setActiveTab}>
          <TabsList className="mb-6">
            <TabsTrigger value="ingestion">
              <Database className="w-4 h-4" />
              New Ingestion
            </TabsTrigger>
            <TabsTrigger value="jobs">
              <List className="w-4 h-4" />
              Jobs
            </TabsTrigger>
          </TabsList>

          <TabsContent value="ingestion">
            <Card className="animate-slide-up" hover>
              <IngestionForm onSuccess={handleIngestionSuccess} />
            </Card>
          </TabsContent>

          <TabsContent value="jobs">
            <div className="animate-slide-up space-y-6">
              <StatsGrid />
              <Card glass>
                <JobList />
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AppContent />
    </QueryClientProvider>
  )
}

export default App
