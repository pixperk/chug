import { useMemo } from 'react';
import { Database, Clock, CheckCircle2, XCircle, ArrowRight } from 'lucide-react';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './ui/Tabs';
import { Badge } from './ui/Badge';
import { Progress } from './ui/Progress';
import { useJobs } from '../hooks/useJobs';
import type { IngestionJob } from '../types/api';

function JobItem({ job }: { job: IngestionJob }) {
  const totalRows = (job.results || []).reduce((sum, r) => sum + (r?.rows || 0), 0);
  const completedTables = (job.results || []).filter(r => !r.error).length;
  const progress = (job.tables || []).length > 0 ? (completedTables / job.tables.length) * 100 : 0;

  const statusConfig = {
    pending: { variant: 'default' as const, icon: Clock, color: 'text-gray-400', pulse: false },
    running: { variant: 'info' as const, icon: ArrowRight, color: 'text-accent', pulse: true },
    completed: { variant: 'success' as const, icon: CheckCircle2, color: 'text-success', pulse: false },
    failed: { variant: 'error' as const, icon: XCircle, color: 'text-error', pulse: false },
  };

  const config = statusConfig[job.status as keyof typeof statusConfig];
  const StatusIcon = config.icon;

  return (
    <div className="group bg-gray-900 border border-gray-800 rounded-lg p-4 hover:border-gray-700 transition-all duration-200">
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1">
            <code className="text-xs text-gray-500 font-mono">{job.id.slice(0, 8)}</code>
            <Badge variant={config.variant} pulse={config.pulse}>
              <StatusIcon className="w-3 h-3" />
              {job.status}
            </Badge>
          </div>
          <div className="flex items-center gap-2 text-sm text-gray-400">
            <Database className="w-3.5 h-3.5" />
            <span>{(job.tables || []).join(', ') || 'No tables'}</span>
          </div>
        </div>

        {/* Row Count */}
        <div className="text-right">
          <div className="text-2xl font-semibold text-foreground">
            {totalRows.toLocaleString()}
          </div>
          <div className="text-xs text-gray-500">rows transferred</div>
        </div>
      </div>

      {/* Progress */}
      {job.status === 'running' && (job.tables || []).length > 0 && (
        <div className="mb-3">
          <Progress value={progress} showLabel size="sm" />
          <p className="text-xs text-gray-500 mt-1.5">
            {completedTables} of {(job.tables || []).length} tables completed
          </p>
        </div>
      )}

      {/* Latest Progress */}
      {(job.progress || []).length > 0 && (
        <div className="mt-3 pt-3 border-t border-gray-800">
          <div className="space-y-1 max-h-24 overflow-y-auto">
            {(job.progress || []).slice(-3).reverse().map((update, i) => (
              <div key={i} className="flex items-center gap-2 text-xs">
                <span className="text-gray-600">
                  {new Date(update.timestamp).toLocaleTimeString()}
                </span>
                <span className="text-gray-500">•</span>
                <span className={`font-medium ${
                  update.event === 'completed' ? 'text-success' :
                  update.event === 'error' ? 'text-error' :
                  'text-accent'
                }`}>
                  {update.event}
                </span>
                <span className="text-gray-400">{update.table}</span>
                {update.row_count && (
                  <span className="text-gray-500">
                    ({update.row_count.toLocaleString()} rows)
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export function JobList() {
  const { data, isLoading } = useJobs();

  const jobs = useMemo(() => {
    if (!data?.jobs) return { all: [], running: [], completed: [], failed: [] };

    return {
      all: data.jobs,
      running: data.jobs.filter(j => j.status === 'running' || j.status === 'pending'),
      completed: data.jobs.filter(j => j.status === 'completed'),
      failed: data.jobs.filter(j => j.status === 'failed'),
    };
  }, [data]);

  if (isLoading) {
    return (
      <div className="text-center py-12">
        <div className="animate-pulse">
          <Database className="w-12 h-12 text-gray-700 mx-auto mb-3" />
          <p className="text-sm text-gray-500">Loading jobs...</p>
        </div>
      </div>
    );
  }

  if (jobs.all.length === 0) {
    return (
      <div className="text-center py-12">
        <Database className="w-12 h-12 text-gray-700 mx-auto mb-3" />
        <p className="text-sm text-gray-500">
          No jobs yet. Start an ingestion to see real-time progress.
        </p>
      </div>
    );
  }

  return (
    <Tabs defaultValue="all" className="space-y-4">
      <TabsList>
        <TabsTrigger value="all">
          All <span className="ml-1.5 text-xs text-gray-500">({jobs.all.length})</span>
        </TabsTrigger>
        <TabsTrigger value="running">
          Running <span className="ml-1.5 text-xs text-gray-500">({jobs.running.length})</span>
        </TabsTrigger>
        <TabsTrigger value="completed">
          Completed <span className="ml-1.5 text-xs text-gray-500">({jobs.completed.length})</span>
        </TabsTrigger>
        <TabsTrigger value="failed">
          Failed <span className="ml-1.5 text-xs text-gray-500">({jobs.failed.length})</span>
        </TabsTrigger>
      </TabsList>

      <TabsContent value="all">
        <div className="space-y-3">
          {jobs.all.map(job => <JobItem key={job.id} job={job} />)}
        </div>
      </TabsContent>

      <TabsContent value="running">
        <div className="space-y-3">
          {jobs.running.length > 0 ? (
            jobs.running.map(job => <JobItem key={job.id} job={job} />)
          ) : (
            <div className="text-center py-8 text-sm text-gray-500">
              No running jobs
            </div>
          )}
        </div>
      </TabsContent>

      <TabsContent value="completed">
        <div className="space-y-3">
          {jobs.completed.length > 0 ? (
            jobs.completed.map(job => <JobItem key={job.id} job={job} />)
          ) : (
            <div className="text-center py-8 text-sm text-gray-500">
              No completed jobs
            </div>
          )}
        </div>
      </TabsContent>

      <TabsContent value="failed">
        <div className="space-y-3">
          {jobs.failed.length > 0 ? (
            jobs.failed.map(job => <JobItem key={job.id} job={job} />)
          ) : (
            <div className="text-center py-8 text-sm text-gray-500">
              No failed jobs
            </div>
          )}
        </div>
      </TabsContent>
    </Tabs>
  );
}
