// TypeScript types matching Go API structs

export interface ProgressUpdate {
  job_id: string;
  table: string;
  event: 'started' | 'extracting' | 'inserting' | 'completed' | 'error';
  message: string;
  row_count?: number;      // Total rows processed for this table
  current_rows?: number;   // Current batch/progress
  total_rows?: number;     // Expected total (from limit)
  percentage?: number;     // Completion percentage
  phase?: string;          // extracting, inserting, completed
  duration?: string;
  timestamp: string;
}

export interface TableResult {
  name: string;
  rows: number;
  duration: string;
  error?: string;
}

export interface TableProgress {
  name: string;
  status: 'pending' | 'extracting' | 'inserting' | 'completed' | 'failed';
  current_rows: number;
  total_rows?: number;
  percentage: number;
  error?: string;
  latest_update?: ProgressUpdate;
}

export interface IngestionJob {
  id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  tables: string[];
  table_configs?: TableConfigRequest[]; // Table configurations for UI display
  results: TableResult[];
  progress: ProgressUpdate[];
  start_time: string;
  end_time?: string;
  error?: string;
  table_progress?: Map<string, TableProgress>; // Client-side only for tracking
}

export interface PollingConfig {
  enabled: boolean;
  delta_column: string;
  interval_seconds: number;
}

export interface TableConfigRequest {
  name: string;
  limit?: number;
  batch_size?: number;
  polling?: PollingConfig;
}

export interface IngestRequest {
  tables: TableConfigRequest[];
  pg_url?: string;
  ch_url?: string;
  limit?: number;        // Default for tables without specific config
  batch_size?: number;   // Default batch size
  polling?: PollingConfig; // Default polling config
}

export interface ConnectionTestResult {
  success: boolean;
  error?: string;
}

export interface ConnectionTestResponse {
  postgresql: ConnectionTestResult;
  clickhouse: ConnectionTestResult;
}

export interface Column {
  name: string;
  data_type: string;
}

export interface TablesResponse {
  tables: string[];
}

export interface ColumnsResponse {
  columns: Column[];
}

export interface JobsResponse {
  jobs: IngestionJob[];
}

export interface JobResponse {
  job: IngestionJob;
}

export interface CreateJobResponse {
  job_id: string;
}
