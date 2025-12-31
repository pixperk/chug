import { apiClient } from './client';
import type {
  IngestRequest,
  CreateJobResponse,
  JobsResponse,
  JobResponse,
} from '../types/api';

export const fetchJobs = async (): Promise<JobsResponse> => {
  return apiClient.get<JobsResponse>('/api/v1/jobs');
};

export const fetchJob = async (id: string): Promise<JobResponse> => {
  return apiClient.get<JobResponse>(`/api/v1/jobs/${id}`);
};

export const createJob = async (req: IngestRequest): Promise<CreateJobResponse> => {
  return apiClient.post<CreateJobResponse>('/api/v1/ingest', req);
};
