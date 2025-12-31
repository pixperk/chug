import { useState } from 'react';
import { Database, Play, ArrowRight } from 'lucide-react';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { Checkbox } from './ui/Checkbox';
import { useTables } from '../hooks/useTables';
import { useCreateJob } from '../hooks/useJobs';
import { TableConfigItem } from './TableConfigItem';
import type { TableConfigRequest } from '../types/api';

interface IngestionFormProps {
  onSuccess?: () => void;
}

export function IngestionForm({ onSuccess }: IngestionFormProps) {
  const [pgUrl, setPgUrl] = useState('');
  const [chUrl, setChUrl] = useState('');
  const [tableConfigs, setTableConfigs] = useState<Map<string, Omit<TableConfigRequest, 'name'>>>(new Map());

  const { data: tablesData, refetch: loadTables, isLoading: isLoadingTables } = useTables(pgUrl);
  const createJobMutation = useCreateJob();

  const handleLoadTables = async () => {
    if (!pgUrl) return;
    await loadTables();
  };

  const handleTableToggle = (tableName: string, checked: boolean) => {
    const newConfigs = new Map(tableConfigs);
    if (checked) {
      newConfigs.set(tableName, {});
    } else {
      newConfigs.delete(tableName);
    }
    setTableConfigs(newConfigs);
  };

  const handleTableConfigChange = (tableName: string, config: Omit<TableConfigRequest, 'name'>) => {
    const newConfigs = new Map(tableConfigs);
    newConfigs.set(tableName, config);
    setTableConfigs(newConfigs);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (tableConfigs.size === 0) {
      alert('Please select at least one table');
      return;
    }

    // Check if any table with CDC enabled is missing a delta column
    for (const [tableName, config] of tableConfigs.entries()) {
      if (config.polling?.enabled && !config.polling.delta_column) {
        alert(`Please select a delta column for table: ${tableName}`);
        return;
      }
    }

    // Convert Map to array of TableConfigRequest
    const tables: TableConfigRequest[] = Array.from(tableConfigs.entries()).map(([name, config]) => ({
      name,
      ...config,
    }));

    createJobMutation.mutate({
      tables,
      pg_url: pgUrl || undefined,
      ch_url: chUrl || undefined,
    }, {
      onSuccess: () => {
        setTableConfigs(new Map());
        if (onSuccess) onSuccess();
      }
    });
  };

  const selectedTables = Array.from(tableConfigs.keys());

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

        {/* Table Selection */}
        <div className="w-full bg-gray-900 border border-gray-800 rounded-lg p-2 min-h-[120px] max-h-[200px] overflow-y-auto custom-scrollbar">
          {tablesData?.tables && tablesData.tables.length > 0 ? (
            <div className="space-y-1">
              {tablesData.tables.map((table) => (
                <div
                  key={table}
                  className="px-2 py-1.5 rounded hover:bg-gray-800/50 transition-colors"
                >
                  <Checkbox
                    checked={tableConfigs.has(table)}
                    onChange={(e) => handleTableToggle(table, e.target.checked)}
                    label={table}
                  />
                </div>
              ))}
            </div>
          ) : (
            <div className="flex items-center justify-center h-full text-sm text-gray-500">
              Click "Load Tables" to fetch available tables
            </div>
          )}
        </div>
      </div>

      {/* Per-Table Configuration */}
      {selectedTables.length > 0 && (
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-3">
            Table Configuration
          </label>
          <div className="space-y-2">
            {selectedTables.map((tableName) => (
              <TableConfigItem
                key={tableName}
                tableName={tableName}
                config={tableConfigs.get(tableName)!}
                onChange={(config) => handleTableConfigChange(tableName, config)}
                onRemove={() => handleTableToggle(tableName, false)}
                pgUrl={pgUrl}
              />
            ))}
          </div>
        </div>
      )}

      <Button
        type="submit"
        className="w-full"
        loading={createJobMutation.isPending}
        disabled={tableConfigs.size === 0}
      >
        <Play className="w-4 h-4" />
        Start Ingestion
        <ArrowRight className="w-4 h-4" />
      </Button>

      {createJobMutation.isSuccess && (
        <div className="bg-success/10 border border-success/20 rounded-lg p-3">
          <p className="text-sm text-success">
            Job created successfully! Check Jobs tab.
          </p>
        </div>
      )}
    </form>
  );
}
