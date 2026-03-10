import axios from 'axios';
import type {
  PipelineState,
  DashboardStats,
  TimeSeriesData,
  PipelineHistory,
  PipelineConfig,
  RunPipelineRequest,
  RunPipelineResponse,
  HealthStatus,
} from '../types';

const api = axios.create({
  baseURL: '/api',
  timeout: 60000, // Increased to 60 seconds for DB2 queries
});

export const pipelinesApi = {
  getAll: () => api.get<PipelineState[]>('/pipelines').then((r) => r.data),
  getOne: (name: string) => api.get<PipelineState>(`/pipelines/${name}`).then((r) => r.data),
  getHistory: (name: string) => api.get<PipelineHistory[]>(`/pipelines/${name}/history`).then((r) => r.data),
  getConfig: (name: string) => api.get<PipelineConfig>(`/pipelines/${name}/config`).then((r) => r.data),
  run: (name: string, data?: RunPipelineRequest) =>
    api.post<RunPipelineResponse>(`/pipelines/${name}/run`, data).then((r) => r.data),
  stop: (name: string) => api.post<{ success: boolean; message: string }>(`/pipelines/${name}/stop`).then((r) => r.data),
  updateConfig: (name: string, config: Partial<PipelineConfig>) =>
    api.put<{ success: boolean; message: string }>(`/pipelines/${name}/config`, config).then((r) => r.data),
  clearQueue: (name: string) =>
    api.delete<{ success: boolean; message: string }>(`/pipelines/${name}/queue`).then((r) => r.data),
  getRunning: () => api.get<string[]>('/pipelines/running/all').then((r) => r.data),
};

export const statsApi = {
  getDashboard: () => api.get<DashboardStats>('/stats').then((r) => r.data),
  getRecordsOverTime: (days?: number) =>
    api.get<TimeSeriesData[]>('/stats/records-over-time', { params: { days } }).then((r) => r.data),
  getTopPipelines: (limit?: number) =>
    api.get<{ name: string; records: number; lastRun: string; status: string }[]>(
      '/stats/top-pipelines',
      { params: { limit } },
    ).then((r) => r.data),
  getRecentFailures: (limit?: number) =>
    api.get<{ name: string; lastRun: string; errorMessage: string }[]>('/stats/recent-failures', {
      params: { limit },
    }).then((r) => r.data),
  getHealth: () => api.get<HealthStatus>('/stats/health').then((r) => r.data),
};

export default api;