import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type { ProgressUpdate, IngestionJob, TableProgress } from '../types/api';

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const queryClient = useQueryClient();
  const reconnectTimeoutRef = useRef<number>();

  useEffect(() => {
    const connect = () => {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

      ws.onopen = () => {
        console.log('WebSocket connected');
      };

      ws.onmessage = (event) => {
        try {
          const update: ProgressUpdate = JSON.parse(event.data);
          handleProgressUpdate(update);
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error);
        }
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
      };

      ws.onclose = () => {
        console.log('WebSocket disconnected, reconnecting in 3s...');
        wsRef.current = null;
        reconnectTimeoutRef.current = setTimeout(connect, 3000);
      };

      wsRef.current = ws;
    };

    const handleProgressUpdate = (update: ProgressUpdate) => {
      queryClient.setQueryData(['jobs'], (oldData: any) => {
        if (!oldData?.jobs) return oldData;

        const updatedJobs = oldData.jobs.map((job: IngestionJob) => {
          if (job.id !== update.job_id) return job;

          const newProgress = [...(job.progress || []), update];
          const tableProgress = job.table_progress || new Map<string, TableProgress>();

          const currentTableProgress = tableProgress.get(update.table) || {
            name: update.table,
            status: 'pending',
            current_rows: 0,
            percentage: 0,
          };

          const updatedTableProgress: TableProgress = {
            ...currentTableProgress,
            current_rows: update.current_rows || update.row_count || currentTableProgress.current_rows,
            total_rows: update.total_rows || currentTableProgress.total_rows,
            percentage: update.percentage || currentTableProgress.percentage,
            status: update.event === 'completed' ? 'completed'
                  : update.event === 'error' ? 'failed'
                  : update.phase as any || currentTableProgress.status,
            error: update.event === 'error' ? update.message : undefined,
            latest_update: update,
          };

          tableProgress.set(update.table, updatedTableProgress);

          return {
            ...job,
            progress: newProgress,
            table_progress: tableProgress,
          };
        });

        return { ...oldData, jobs: updatedJobs };
      });
    };

    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [queryClient]);

  return wsRef;
}
