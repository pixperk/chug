// TypeScript types matching Go API structs

export interface ProgressUpdate {
  job_id: string;
  table: string;
  event: 'started' | 'extracting' | 'inserting' | 'completed' | 'error';
  message: string;
  row_count?: number;
  duration?: string;
  timestamp: string;
}

export interface TableResult {
  name: string;
  rows: number;
  duration: string;
  error?: string;
}

export interface IngestionJob {
  id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  tables: string[];
  results: TableResult[];
  progress: ProgressUpdate[];
  start_time: string;
  end_time?: string;
  error?: string;
}

export interface IngestRequest {
  tables: string[];
  pg_url?: string;
  ch_url?: string;
  limit?: number;
  batch_size?: number;
}

export interface ConnectionTestResult {
  success: boolean;
  error?: string;
}

export interface ConnectionTestResponse {
  postgresql: ConnectionTestResult;
  clickhouse: ConnectionTestResult;
}

export interface TablesResponse {
  tables: string[];
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
