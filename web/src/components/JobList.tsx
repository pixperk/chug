import { Clock, CheckCircle2, XCircle, ArrowRight, Database, RefreshCw } from 'lucide-react';
import { Badge } from './ui/Badge';
import { Progress } from './ui/Progress';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './ui/Tabs';
import { useJobs } from '../hooks/useJobs';
import type { IngestionJob, TableProgress, PollingConfig } from '../types/api';

interface TableProgressItemProps {
  tableProgress: TableProgress;
  polling?: PollingConfig;
}

function TableProgressItem({ tableProgress, polling }: TableProgressItemProps) {
  const statusConfig = {
    pending: { icon: Clock, color: 'text-gray-400', bgColor: 'bg-gray-800/30' },
    extracting: { icon: ArrowRight, color: 'text-blue-400', bgColor: 'bg-blue-500/10' },
    inserting: { icon: ArrowRight, color: 'text-accent', bgColor: 'bg-accent/10' },
    completed: { icon: CheckCircle2, color: 'text-success', bgColor: 'bg-success/10' },
    failed: { icon: XCircle, color: 'text-error', bgColor: 'bg-error/10' },
  };

  const config = statusConfig[tableProgress.status] || statusConfig.pending;
  const Icon = config.icon;

  return (
    <div className={`p-3 rounded-lg border border-gray-800 ${config.bgColor} transition-all duration-200`}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <Database className="w-3.5 h-3.5 text-gray-500" />
          <span className="text-sm font-medium text-foreground">{tableProgress.name}</span>
          {polling?.enabled && (
            <span className="inline-flex items-center gap-1 px-1.5 py-0.5 text-xs bg-accent/10 text-accent rounded-full">
              <RefreshCw className="w-3 h-3" />
              CDC
            </span>
          )}
        </div>
        <div className={`flex items-center gap-1.5 text-xs ${config.color}`}>
          <Icon className="w-3.5 h-3.5" />
          <span className="capitalize">{tableProgress.status}</span>
        </div>
      </div>

      {polling?.enabled && (
        <div className="mb-2 text-xs text-gray-500 space-y-0.5">
          <div>Tracking: <span className="text-gray-400 font-mono">{polling.delta_column || 'N/A'}</span></div>
          <div>Interval: <span className="text-gray-400">{polling.interval_seconds || 0}s</span></div>
        </div>
      )}

      {(tableProgress.status === 'extracting' || tableProgress.status === 'inserting') && (
        <div className="mb-2">
          <Progress value={tableProgress.percentage} size="sm" />
        </div>
      )}

      <div className="flex items-center justify-between text-xs">
        <span className="text-gray-500">
          {tableProgress.total_rows
            ? `${tableProgress.current_rows.toLocaleString()} / ${tableProgress.total_rows.toLocaleString()} rows`
            : `${tableProgress.current_rows.toLocaleString()} rows`
          }
        </span>
        {tableProgress.percentage > 0 && (
          <span className="font-medium text-accent">
            {tableProgress.percentage.toFixed(1)}%
          </span>
        )}
      </div>

      {tableProgress.error && (
        <div className="mt-2 text-xs text-error">{tableProgress.error}</div>
      )}
    </div>
  );
}

function JobItem({ job }: { job: IngestionJob }) {
  const tableProgressMap = new Map<string, TableProgress>();

  // Create a map of table name -> polling config
  const tablePollingMap = new Map<string, PollingConfig>();
  (job.table_configs || []).forEach(config => {
    if (config.polling?.enabled) {
      tablePollingMap.set(config.name, config.polling);
    }
  });

  job.tables.forEach(tableName => {
    tableProgressMap.set(tableName, {
      name: tableName,
      status: 'pending',
      current_rows: 0,
      percentage: 0,
    });
  });

  (job.results || []).forEach(result => {
    tableProgressMap.set(result.name, {
      name: result.name,
      status: result.error ? 'failed' : 'completed',
      current_rows: result.rows,
      percentage: 100,
      error: result.error,
    });
  });

  const latestProgressByTable = new Map<string, typeof job.progress[0]>();
  (job.progress || []).forEach(update => {
    latestProgressByTable.set(update.table, update);
  });

  latestProgressByTable.forEach((update, tableName) => {
    const current = tableProgressMap.get(tableName);
    if (current && current.status !== 'completed' && current.status !== 'failed') {
      tableProgressMap.set(tableName, {
        name: tableName,
        status: update.event === 'error' ? 'failed'
              : update.phase as any || 'extracting',
        current_rows: update.current_rows || update.row_count || 0,
        total_rows: update.total_rows,
        percentage: update.percentage || 0,
        error: update.event === 'error' ? update.message : undefined,
        latest_update: update,
      });
    }
  });

  const tableProgressList = Array.from(tableProgressMap.values());
  const completedTables = tableProgressList.filter(t => t.status === 'completed').length;
  const failedTables = tableProgressList.filter(t => t.status === 'failed').length;
  const totalTables = job.tables.length;
  const overallPercentage = totalTables > 0 ? (completedTables / totalTables) * 100 : 0;
  const totalRows = tableProgressList.reduce((sum, t) => sum + t.current_rows, 0);

  const statusConfig = {
    pending: { variant: 'default' as const, icon: Clock, color: 'text-gray-400', pulse: false },
    running: { variant: 'info' as const, icon: ArrowRight, color: 'text-accent', pulse: true },
    completed: { variant: 'success' as const, icon: CheckCircle2, color: 'text-success', pulse: false },
    failed: { variant: 'error' as const, icon: XCircle, color: 'text-error', pulse: false },
  };

  const config = statusConfig[job.status as keyof typeof statusConfig] || statusConfig.pending;
  const StatusIcon = config.icon;

  return (
    <div className="group bg-gray-900 border border-gray-800 rounded-lg p-4 hover:border-gray-700 transition-all duration-200">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <span className="text-sm font-medium text-gray-400">Job ID:</span>
              <span className="text-sm font-mono text-foreground">{job.id}</span>
            </div>
            <div className="text-xs text-gray-500">
              Started: {new Date(job.start_time).toLocaleString()}
            </div>
          </div>
        </div>

        <Badge variant={config.variant} pulse={config.pulse}>
          <StatusIcon className="w-3 h-3" />
          {job.status}
        </Badge>
      </div>

      <div className="mb-4 p-3 bg-gray-800/30 rounded-lg border border-gray-800">
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm font-medium text-gray-300">Overall Progress</span>
          <span className="text-sm text-gray-400">
            {completedTables} / {totalTables} tables
          </span>
        </div>
        <Progress value={overallPercentage} showLabel size="md" />
        <div className="flex items-center justify-between mt-2 text-xs text-gray-500">
          <span>{totalRows.toLocaleString()} total rows transferred</span>
          {failedTables > 0 && (
            <span className="text-error">{failedTables} failed</span>
          )}
        </div>
      </div>

      <div>
        <div className="text-xs font-medium text-gray-400 mb-2 uppercase tracking-wide">
          Tables ({totalTables})
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          {tableProgressList.map(tableProgress => (
            <TableProgressItem
              key={tableProgress.name}
              tableProgress={tableProgress}
              polling={tablePollingMap.get(tableProgress.name)}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

export function JobList() {
  const { data, isLoading } = useJobs();

  const jobs = {
    all: data?.jobs || [],
    running: (data?.jobs || []).filter(j => j.status === 'running' || j.status === 'pending'),
    completed: (data?.jobs || []).filter(j => j.status === 'completed'),
    failed: (data?.jobs || []).filter(j => j.status === 'failed'),
  };

  if (isLoading) {
    return <div className="text-center text-gray-500 py-12">Loading jobs...</div>;
  }

  if (jobs.all.length === 0) {
    return (
      <div className="text-center py-12">
        <Database className="w-12 h-12 text-gray-700 mx-auto mb-3" />
        <p className="text-sm text-gray-500">
          No jobs yet. Start an ingestion to see progress here.
        </p>
      </div>
    );
  }

  return (
    <Tabs defaultValue="all">
      <TabsList>
        <TabsTrigger value="all">All ({jobs.all.length})</TabsTrigger>
        <TabsTrigger value="running">Running ({jobs.running.length})</TabsTrigger>
        <TabsTrigger value="completed">Completed ({jobs.completed.length})</TabsTrigger>
        <TabsTrigger value="failed">Failed ({jobs.failed.length})</TabsTrigger>
      </TabsList>

      <TabsContent value="all">
        <div className="space-y-4">
          {jobs.all.map(job => (
            <JobItem key={job.id} job={job} />
          ))}
        </div>
      </TabsContent>

      <TabsContent value="running">
        {jobs.running.length > 0 ? (
          <div className="space-y-4">
            {jobs.running.map(job => (
              <JobItem key={job.id} job={job} />
            ))}
          </div>
        ) : (
          <div className="text-center py-12 text-gray-500">No running jobs</div>
        )}
      </TabsContent>

      <TabsContent value="completed">
        {jobs.completed.length > 0 ? (
          <div className="space-y-4">
            {jobs.completed.map(job => (
              <JobItem key={job.id} job={job} />
            ))}
          </div>
        ) : (
          <div className="text-center py-12 text-gray-500">No completed jobs</div>
        )}
      </TabsContent>

      <TabsContent value="failed">
        {jobs.failed.length > 0 ? (
          <div className="space-y-4">
            {jobs.failed.map(job => (
              <JobItem key={job.id} job={job} />
            ))}
          </div>
        ) : (
          <div className="text-center py-12 text-gray-500">No failed jobs</div>
        )}
      </TabsContent>
    </Tabs>
  );
}
