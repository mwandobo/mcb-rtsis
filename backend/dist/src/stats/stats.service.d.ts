import { Repository } from 'typeorm';
import { PipelineState } from '../pipelines/entities/pipeline-state.entity';
import { PipelinesService } from '../pipelines/pipelines.service';
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
export declare class StatsService {
    private readonly pipelineStateRepository;
    private readonly pipelinesService;
    constructor(pipelineStateRepository: Repository<PipelineState>, pipelinesService: PipelinesService);
    getDashboardStats(): Promise<DashboardStats>;
    getRecordsOverTime(days?: number): Promise<TimeSeriesData[]>;
    getTopPipelines(limit?: number): Promise<any[]>;
    getRecentFailures(limit?: number): Promise<any[]>;
    getHealthStatus(): Promise<{
        status: 'healthy' | 'warning' | 'critical';
        checks: Record<string, any>;
    }>;
}
