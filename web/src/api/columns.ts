import { apiClient } from './client';
import type { ColumnsResponse } from '../types/api';

export const fetchColumns = (table: string, pgUrl?: string): Promise<ColumnsResponse> => {
  const params = new URLSearchParams({ table });
  if (pgUrl) params.append('pg_url', pgUrl);
  return apiClient.get<ColumnsResponse>(`/api/v1/tables/columns?${params.toString()}`);
};
