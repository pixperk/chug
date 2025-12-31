import { apiClient } from './client';
import type { ConnectionTestResponse } from '../types/api';

export const testConnection = async (params: {
  pg_url?: string;
  ch_url?: string;
}): Promise<ConnectionTestResponse> => {
  return apiClient.post<ConnectionTestResponse>('/api/v1/test-connection', params);
};
