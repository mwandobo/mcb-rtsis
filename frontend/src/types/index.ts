export interface PipelineState {
  pipelineName: string;
  lastRun: string | null;
  lastSuccessfulRun: string | null;
  lastRunStatus: string | null;
  recordsProcessed: number;
  errorMessage: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface DashboardStats {
  totalPipelines: number;
  runningPipelines: number;
  failedToday: number;
  successRate: number;
  totalRecordsToday: number;
  pipelinesByStatus: Record<string, number>;
}

export interface TimeSeriesData {
  timestamp: string;
  records: number;
  pipelines: number;
}

export interface PipelineHistory {
  runTime: string | null;
  status: string | null;
  records: number;
  errorMessage: string | null;
}

export interface PipelineConfig {
  pipelineName: string;
  mode: string;
  schedule: string;
  source: string;
  queue: string;
}

export interface RunPipelineRequest {
  fullLoad?: boolean;
}

export interface RunPipelineResponse {
  success: boolean;
  pipelineName: string;
  processId: number;
  message: string;
}

export interface HealthStatus {
  status: 'healthy' | 'warning' | 'critical';
  checks: Record<string, { status: string; message?: string; count?: number }>;
}