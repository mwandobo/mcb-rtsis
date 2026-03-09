import { PipelinesService, RunPipelineDto } from './pipelines.service';
import { PipelineState } from './entities/pipeline-state.entity';
export declare class PipelinesController {
    private readonly pipelinesService;
    constructor(pipelinesService: PipelinesService);
    findAll(): Promise<PipelineState[]>;
    findOne(name: string): Promise<PipelineState>;
    getHistory(name: string): Promise<any[]>;
    getConfig(name: string): Promise<any>;
    runPipeline(name: string, dto: RunPipelineDto): Promise<import("./pipelines.service").PipelineRunResult>;
    stopPipeline(name: string): Promise<{
        success: boolean;
        message: string;
    }>;
    updateConfig(name: string, config: any): Promise<{
        success: boolean;
        message: string;
    }>;
    clearQueue(name: string): Promise<{
        success: boolean;
        message: string;
    }>;
    getRunningPipelines(): Promise<string[]>;
}
