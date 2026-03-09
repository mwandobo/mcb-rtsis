import { Repository } from 'typeorm';
import { PipelineState } from '../pipelines/entities/pipeline-state.entity';
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
    constructor(pipelineStateRepository: Repository<PipelineState>);
    getDashboardStats(): Promise<DashboardStats>;
    getRecordsOverTime(days?: number): Promise<TimeSeriesData[]>;
    getTopPipelines(limit?: number): Promise<any[]>;
    getRecentFailures(limit?: number): Promise<any[]>;
    getHealthStatus(): Promise<{
        status: 'healthy' | 'warning' | 'critical';
        checks: Record<string, any>;
    }>;
}
