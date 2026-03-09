import { StatsService } from './stats.service';
export declare class StatsController {
    private readonly statsService;
    constructor(statsService: StatsService);
    getDashboardStats(): Promise<import("./stats.service").DashboardStats>;
    getRecordsOverTime(days?: number): Promise<import("./stats.service").TimeSeriesData[]>;
    getTopPipelines(limit?: number): Promise<any[]>;
    getRecentFailures(limit?: number): Promise<any[]>;
    getHealthStatus(): Promise<{
        status: "healthy" | "warning" | "critical";
        checks: Record<string, any>;
    }>;
}
