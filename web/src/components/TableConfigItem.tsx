import { useState } from 'react';
import { ChevronDown, ChevronRight, RefreshCw, X } from 'lucide-react';
import { NumberInput } from './ui/NumberInput';
import { Checkbox } from './ui/Checkbox';
import { useColumns } from '../hooks/useColumns';
import type { TableConfigRequest } from '../types/api';

interface TableConfigItemProps {
  tableName: string;
  config: Omit<TableConfigRequest, 'name'>;
  onChange: (config: Omit<TableConfigRequest, 'name'>) => void;
  onRemove: () => void;
  pgUrl?: string;
}

export function TableConfigItem({ tableName, config, onChange, onRemove, pgUrl }: TableConfigItemProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const { data: columnsData } = useColumns(tableName, config.polling?.enabled ? pgUrl : undefined);

  const updateConfig = (updates: Partial<Omit<TableConfigRequest, 'name'>>) => {
    onChange({ ...config, ...updates });
  };

  return (
    <div className="border border-gray-800 rounded-lg bg-gray-900/50">
      {/* Header */}
      <div className="flex items-center justify-between p-3 cursor-pointer hover:bg-gray-800/50 transition-colors" onClick={() => setIsExpanded(!isExpanded)}>
        <div className="flex items-center gap-2">
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-gray-400" />
          ) : (
            <ChevronRight className="w-4 h-4 text-gray-400" />
          )}
          <span className="text-sm font-medium text-foreground">{tableName}</span>
          {config.polling?.enabled && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-accent/10 text-accent rounded-full">
              <RefreshCw className="w-3 h-3" />
              CDC
            </span>
          )}
        </div>
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onRemove();
          }}
          className="p-1 text-gray-400 hover:text-error hover:bg-error/10 rounded transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Expanded Config */}
      {isExpanded && (
        <div className="p-4 pt-2 space-y-4 border-t border-gray-800">
          {/* Limit and Batch Size */}
          <div className="grid grid-cols-2 gap-4">
            <NumberInput
              label="Limit"
              placeholder="Default"
              value={config.limit?.toString() || ''}
              onChange={(val) => updateConfig({ limit: val ? parseInt(val) : undefined })}
              min={1}
              hint="Rows per run"
            />
            <NumberInput
              label="Batch Size"
              placeholder="Default"
              value={config.batch_size?.toString() || ''}
              onChange={(val) => updateConfig({ batch_size: val ? parseInt(val) : undefined })}
              min={1}
              hint="Rows per batch"
            />
          </div>

          {/* CDC/Polling */}
          <div className="border border-gray-800 rounded-lg p-3 space-y-3 bg-gray-900/30">
            <Checkbox
              checked={config.polling?.enabled || false}
              onChange={(e) => updateConfig({
                polling: e.target.checked
                  ? { enabled: true, delta_column: '', interval_seconds: 60 }
                  : undefined
              })}
              label={
                <span className="inline-flex items-center gap-1.5">
                  <RefreshCw className="w-3.5 h-3.5" />
                  Enable CDC
                </span>
              }
            />

            {config.polling?.enabled && (
              <div className="grid grid-cols-2 gap-3 pt-2 border-t border-gray-800">
                <div>
                  <label className="block text-xs font-medium text-gray-400 mb-1.5">
                    Delta Column
                  </label>
                  <select
                    value={config.polling.delta_column}
                    onChange={(e) => updateConfig({
                      polling: { ...config.polling!, delta_column: e.target.value }
                    })}
                    className="w-full px-2 py-1.5 text-sm bg-gray-900 border border-gray-800 rounded-lg text-foreground focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent transition-all duration-200"
                  >
                    <option value="">Select...</option>
                    {columnsData?.columns?.map((col) => (
                      <option key={col.name} value={col.name}>
                        {col.name}
                      </option>
                    ))}
                  </select>
                </div>

                <NumberInput
                  label="Interval (sec)"
                  placeholder="60"
                  value={config.polling.interval_seconds?.toString() || '60'}
                  onChange={(val) => updateConfig({
                    polling: { ...config.polling!, interval_seconds: parseInt(val) || 60 }
                  })}
                  min={1}
                />
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
