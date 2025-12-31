import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { fetchJobs, createJob } from '../api/jobs';

export function useJobs() {
  return useQuery({
    queryKey: ['jobs'],
    queryFn: fetchJobs,
    refetchInterval: 5000, // Refetch every 5 seconds
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
