import { ConfigService } from '../config/config.service';
export declare class DatabasesService {
    private readonly configService;
    private readonly logger;
    constructor(configService: ConfigService);
    private getDb2Connection;
    getDb2Tables(): Promise<any[]>;
    getPostgresTables(): Promise<any[]>;
    getPostgresTableData(tableName: string, limit?: number, offset?: number): Promise<{
        data: any[];
        total: number;
        columns: string[];
    }>;
    getDb2TableData(pipelineName: string, limit?: number, offset?: number): Promise<{
        data: any[];
        total: number;
        columns: string[];
    }>;
    getPipelineStats(): Promise<any>;
}
