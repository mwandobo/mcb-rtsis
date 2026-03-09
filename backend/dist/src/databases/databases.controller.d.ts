import { DatabasesService } from './databases.service';
export declare class DatabasesController {
    private readonly databasesService;
    private readonly logger;
    constructor(databasesService: DatabasesService);
    getDb2Tables(): Promise<any[]>;
    getDb2TableData(name: string, limit?: number, offset?: number): Promise<{
        data: any[];
        total: number;
        columns: string[];
    }>;
    getPostgresTables(): Promise<any[]>;
    getPostgresTableData(name: string, limit?: number, offset?: number): Promise<{
        data: any[];
        total: number;
        columns: string[];
    }>;
    getPipelineStats(): Promise<any>;
}
