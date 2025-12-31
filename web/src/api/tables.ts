import { apiClient } from './client';
import type { TablesResponse } from '../types/api';

export const fetchTables = async (pgUrl?: string): Promise<TablesResponse> => {
  const params = pgUrl ? `?pg_url=${encodeURIComponent(pgUrl)}` : '';
  return apiClient.get<TablesResponse>(`/api/v1/tables${params}`);
};
