import { useQuery } from '@tanstack/react-query';
import { fetchColumns } from '../api/columns';

export function useColumns(table: string, pgUrl?: string) {
  return useQuery({
    queryKey: ['columns', table, pgUrl],
    queryFn: () => fetchColumns(table, pgUrl),
    enabled: !!table, // Only fetch if table is specified
  });
}
