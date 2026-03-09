import { Repository } from 'typeorm';
import { PipelineState } from './entities/pipeline-state.entity';
import { ConfigService } from '../config/config.service';
export interface RunPipelineDto {
    fullLoad?: boolean;
}
export interface PipelineRunResult {
    success: boolean;
    pipelineName: string;
    processId?: number;
    message: string;
}
export declare class PipelinesService {
    private readonly pipelineStateRepository;
    private readonly configService;
    private readonly logger;
    private runningProcesses;
    constructor(pipelineStateRepository: Repository<PipelineState>, configService: ConfigService);
    findAll(): Promise<PipelineState[]>;
    findOne(name: string): Promise<PipelineState>;
    getHistory(name: string, limit?: number): Promise<any[]>;
    runPipeline(name: string, options?: RunPipelineDto): Promise<PipelineRunResult>;
    stopPipeline(name: string): Promise<{
        success: boolean;
        message: string;
    }>;
    getRunningPipelines(): Promise<string[]>;
    getPipelineConfig(name: string): Promise<any>;
    updateConfig(name: string, config: any): Promise<{
        success: boolean;
        message: string;
    }>;
    clearQueue(name: string): Promise<{
        success: boolean;
        message: string;
    }>;
}
