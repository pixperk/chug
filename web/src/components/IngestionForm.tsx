import { useState } from 'react';
import { Database, Play, ArrowRight, RefreshCw } from 'lucide-react';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { NumberInput } from './ui/NumberInput';
import { useTables } from '../hooks/useTables';
import { useColumns } from '../hooks/useColumns';
import { useCreateJob } from '../hooks/useJobs';

interface IngestionFormProps {
  onSuccess?: () => void;
}

export function IngestionForm({ onSuccess }: IngestionFormProps) {
  const [pgUrl, setPgUrl] = useState('');
  const [chUrl, setChUrl] = useState('');
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [limit, setLimit] = useState('');
  const [batchSize, setBatchSize] = useState('500');

  // CDC/Polling state
  const [enablePolling, setEnablePolling] = useState(false);
  const [pollingInterval, setPollingInterval] = useState('60');
  const [deltaColumn, setDeltaColumn] = useState('');

  const { data: tablesData, refetch: loadTables, isLoading: isLoadingTables } = useTables(pgUrl);
  const { data: columnsData } = useColumns(selectedTables[0] || '', enablePolling ? pgUrl : undefined);
  const createJobMutation = useCreateJob();

  const handleLoadTables = async () => {
    if (!pgUrl) return;
    await loadTables();
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (selectedTables.length === 0) {
      alert('Please select at least one table');
      return;
    }

    if (enablePolling && !deltaColumn) {
      alert('Please select a delta column for CDC/Polling');
      return;
    }

    createJobMutation.mutate({
      tables: selectedTables,
      pg_url: pgUrl || undefined,
      ch_url: chUrl || undefined,
      limit: limit ? parseInt(limit) : undefined,
      batch_size: batchSize ? parseInt(batchSize) : undefined,
      polling: enablePolling ? {
        enabled: true,
        delta_column: deltaColumn,
        interval_seconds: parseInt(pollingInterval) || 60,
      } : undefined,
    }, {
      onSuccess: () => {
        setSelectedTables([]);
        setDeltaColumn('');
        if (onSuccess) onSuccess();
      }
    });
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <Input
        label="PostgreSQL URL"
        placeholder="postgresql://user:pass@host:5432/db"
        value={pgUrl}
        onChange={(e) => setPgUrl(e.target.value)}
        hint="Leave empty to use server default"
      />

      <Input
        label="ClickHouse URL"
        placeholder="clickhouse://host:9000"
        value={chUrl}
        onChange={(e) => setChUrl(e.target.value)}
        hint="Leave empty to use server default"
      />

      <div>
        <div className="flex items-center justify-between mb-2">
          <label className="block text-sm font-medium text-gray-300">
            Tables {selectedTables.length > 0 && (
              <span className="text-xs text-gray-500 ml-2">
                ({selectedTables.length} selected)
              </span>
            )}
          </label>
          <Button
            type="button"
            size="sm"
            variant="secondary"
            onClick={handleLoadTables}
            loading={isLoadingTables}
            disabled={!pgUrl}
          >
            <Database className="w-3.5 h-3.5" />
            Load Tables
          </Button>
        </div>

        <div className="w-full bg-gray-900 border border-gray-800 rounded-lg p-2 min-h-[120px] max-h-[200px] overflow-y-auto custom-scrollbar">
          {tablesData?.tables && tablesData.tables.length > 0 ? (
            <div className="space-y-1">
              {tablesData.tables.map((table) => (
                <label
                  key={table}
                  className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-gray-800/50 cursor-pointer transition-colors group"
                >
                  <input
                    type="checkbox"
                    checked={selectedTables.includes(table)}
                    onChange={(e) => {
                      if (e.target.checked) {
                        setSelectedTables([...selectedTables, table]);
                      } else {
                        setSelectedTables(selectedTables.filter(t => t !== table));
                      }
                    }}
                    className="w-4 h-4 rounded border-gray-700 bg-gray-800 text-accent focus:ring-accent/50 focus:ring-2 cursor-pointer"
                  />
                  <span className="text-sm text-gray-300 group-hover:text-foreground transition-colors">
                    {table}
                  </span>
                </label>
              ))}
            </div>
          ) : (
            <div className="flex items-center justify-center h-full text-sm text-gray-500">
              Click "Load Tables" to fetch available tables
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <NumberInput
          label="Limit"
          placeholder="1000"
          value={limit}
          onChange={setLimit}
          min={1}
          hint="Max rows per table"
        />

        <NumberInput
          label="Batch Size"
          placeholder="500"
          value={batchSize}
          onChange={setBatchSize}
          min={1}
          hint="Rows per batch"
        />
      </div>

      {/* CDC/Polling Configuration */}
      <div className="border border-gray-800 rounded-lg p-4 space-y-4 bg-gray-900/30">
        <div className="flex items-center justify-between">
          <div>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={enablePolling}
                onChange={(e) => setEnablePolling(e.target.checked)}
                className="w-4 h-4 rounded border-gray-700 bg-gray-800 text-accent focus:ring-accent/50 focus:ring-2 cursor-pointer"
              />
              <span className="text-sm font-medium text-gray-300">
                <RefreshCw className="w-4 h-4 inline mr-1" />
                Enable CDC / Polling
              </span>
            </label>
            <p className="text-xs text-gray-500 ml-6 mt-1">
              Continuously sync new data using a delta column
            </p>
          </div>
        </div>

        {enablePolling && (
          <div className="space-y-4 pt-2 border-t border-gray-800">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Delta Column
                </label>
                <select
                  value={deltaColumn}
                  onChange={(e) => setDeltaColumn(e.target.value)}
                  className="w-full px-3 py-2 bg-gray-900 border border-gray-800 rounded-lg text-foreground focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent transition-all duration-200"
                  required={enablePolling}
                >
                  <option value="">Select column...</option>
                  {columnsData?.columns?.map((col) => (
                    <option key={col.name} value={col.name}>
                      {col.name} ({col.data_type})
                    </option>
                  ))}
                </select>
                <p className="mt-1.5 text-xs text-gray-500">
                  Column to track changes (e.g., updated_at)
                </p>
              </div>

              <NumberInput
                label="Polling Interval"
                placeholder="60"
                value={pollingInterval}
                onChange={setPollingInterval}
                min={1}
                hint="Seconds between polls"
              />
            </div>
          </div>
        )}
      </div>

      <Button
        type="submit"
        className="w-full"
        loading={createJobMutation.isPending}
        disabled={selectedTables.length === 0}
      >
        <Play className="w-4 h-4" />
        Start Ingestion
        <ArrowRight className="w-4 h-4" />
      </Button>

      {createJobMutation.isSuccess && (
        <div className="bg-success/10 border border-success/20 rounded-lg p-3">
          <p className="text-sm text-success">
            Job created successfully! Check Active Jobs below.
          </p>
        </div>
      )}
    </form>
  );
}
