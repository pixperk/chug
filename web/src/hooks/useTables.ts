import { useQuery } from '@tanstack/react-query';
import { fetchTables } from '../api/tables';

export function useTables(pgUrl?: string) {
  return useQuery({
    queryKey: ['tables', pgUrl],
    queryFn: () => fetchTables(pgUrl),
    enabled: false, // Only fetch when manually triggered
  });
}
