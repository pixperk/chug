import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Database, Zap } from 'lucide-react'
import { Card } from './components/ui/Card'
import { Badge } from './components/ui/Badge'

const queryClient = new QueryClient()

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="min-h-screen bg-background">
        <div className="max-w-6xl mx-auto px-6 py-8">
          {/* Header */}
          <header className="mb-12 animate-slide-up">
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center gap-3 mb-2">
                  <div className="flex items-center gap-2">
                    <Zap className="w-6 h-6 text-accent" />
                    <h1 className="text-3xl font-semibold tracking-tight text-foreground">
                      Chug
                    </h1>
                  </div>
                  <Badge variant="info">Beta</Badge>
                </div>
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
          </header>

          {/* Main Content */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* New Ingestion Card */}
            <Card className="animate-slide-up" style={{ animationDelay: '100ms' }} hover>
              <div className="flex items-center gap-2 mb-6">
                <Database className="w-5 h-5 text-accent" />
                <h2 className="text-lg font-medium text-foreground">
                  New Ingestion
                </h2>
              </div>

              <div className="space-y-4">
                <div className="bg-gray-800/30 rounded-lg p-4 border border-gray-800">
                  <p className="text-sm text-gray-400 text-center">
                    Configuration panel coming soon...
                  </p>
                </div>
              </div>
            </Card>

            {/* Stats Card */}
            <Card className="animate-slide-up" style={{ animationDelay: '200ms' }} glass>
              <div className="flex items-center gap-2 mb-6">
                <Zap className="w-5 h-5 text-accent" />
                <h2 className="text-lg font-medium text-foreground">
                  Statistics
                </h2>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="bg-gray-800/30 rounded-lg p-4 border border-gray-800 hover:border-gray-700 transition-colors">
                  <p className="text-xs text-gray-500 mb-1">Total Jobs</p>
                  <p className="text-2xl font-semibold text-foreground">0</p>
                </div>
                <div className="bg-gray-800/30 rounded-lg p-4 border border-gray-800 hover:border-gray-700 transition-colors">
                  <p className="text-xs text-gray-500 mb-1">Running</p>
                  <p className="text-2xl font-semibold text-accent">0</p>
                </div>
                <div className="bg-gray-800/30 rounded-lg p-4 border border-gray-800 hover:border-gray-700 transition-colors">
                  <p className="text-xs text-gray-500 mb-1">Completed</p>
                  <p className="text-2xl font-semibold text-success">0</p>
                </div>
                <div className="bg-gray-800/30 rounded-lg p-4 border border-gray-800 hover:border-gray-700 transition-colors">
                  <p className="text-xs text-gray-500 mb-1">Failed</p>
                  <p className="text-2xl font-semibold text-error">0</p>
                </div>
              </div>
            </Card>
          </div>

          {/* Active Jobs */}
          <div className="mt-6 animate-slide-up" style={{ animationDelay: '300ms' }}>
            <Card glass>
              <div className="flex items-center gap-2 mb-6">
                <Zap className="w-5 h-5 text-accent" />
                <h2 className="text-lg font-medium text-foreground">
                  Active Jobs
                </h2>
              </div>

              <div className="text-center py-12">
                <Database className="w-12 h-12 text-gray-700 mx-auto mb-3" />
                <p className="text-sm text-gray-500">
                  No active jobs. Start an ingestion to see real-time progress.
                </p>
              </div>
            </Card>
          </div>
        </div>
      </div>
    </QueryClientProvider>
  )
}

export default App
