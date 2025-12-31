import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { fetchJobs, createJob } from '../api/jobs';

export function useJobs() {
  return useQuery({
    queryKey: ['jobs'],
    queryFn: fetchJobs,
    // WebSocket provides real-time updates, no need for polling
    // Only refetch on window focus if data is stale
    staleTime: 30000, // 30 seconds
  });
}

export function useCreateJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: createJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] });
    },
  });
}
