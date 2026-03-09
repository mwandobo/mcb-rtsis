import {
  Controller,
  Get,
  Post,
  Put,
  Delete,
  Body,
  Param,
  HttpCode,
  HttpStatus,
} from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from '@nestjs/swagger';
import { PipelinesService, RunPipelineDto } from './pipelines.service';
import { PipelineState } from './entities/pipeline-state.entity';

@ApiTags('pipelines')
@Controller('pipelines')
export class PipelinesController {
  constructor(private readonly pipelinesService: PipelinesService) {}

  @Get()
  @ApiOperation({ summary: 'Get all pipelines with their status' })
  @ApiResponse({ status: 200, description: 'List of all pipelines' })
  async findAll(): Promise<PipelineState[]> {
    return this.pipelinesService.findAll();
  }

  @Get(':name')
  @ApiOperation({ summary: 'Get a specific pipeline status' })
  @ApiParam({ name: 'name', description: 'Pipeline name' })
  @ApiResponse({ status: 200, description: 'Pipeline details' })
  @ApiResponse({ status: 404, description: 'Pipeline not found' })
  async findOne(@Param('name') name: string): Promise<PipelineState> {
    return this.pipelinesService.findOne(name);
  }

  @Get(':name/history')
  @ApiOperation({ summary: 'Get pipeline run history' })
  @ApiParam({ name: 'name', description: 'Pipeline name' })
  @ApiResponse({ status: 200, description: 'Pipeline run history' })
  async getHistory(@Param('name') name: string) {
    return this.pipelinesService.getHistory(name);
  }

  @Get(':name/config')
  @ApiOperation({ summary: 'Get pipeline configuration' })
  @ApiParam({ name: 'name', description: 'Pipeline name' })
  @ApiResponse({ status: 200, description: 'Pipeline configuration' })
  async getConfig(@Param('name') name: string) {
    return this.pipelinesService.getPipelineConfig(name);
  }

  @Post(':name/run')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Trigger a pipeline run' })
  @ApiParam({ name: 'name', description: 'Pipeline name' })
  @ApiResponse({ status: 200, description: 'Pipeline started' })
  @ApiResponse({ status: 400, description: 'Pipeline already running' })
  async runPipeline(
    @Param('name') name: string,
    @Body() dto: RunPipelineDto,
  ) {
    return this.pipelinesService.runPipeline(name, dto);
  }

  @Post(':name/stop')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Stop a running pipeline' })
  @ApiParam({ name: 'name', description: 'Pipeline name' })
  @ApiResponse({ status: 200, description: 'Pipeline stopped' })
  async stopPipeline(@Param('name') name: string) {
    return this.pipelinesService.stopPipeline(name);
  }

  @Put(':name/config')
  @ApiOperation({ summary: 'Update pipeline configuration' })
  @ApiParam({ name: 'name', description: 'Pipeline name' })
  @ApiResponse({ status: 200, description: 'Configuration updated' })
  async updateConfig(
    @Param('name') name: string,
    @Body() config: any,
  ) {
    return this.pipelinesService.updateConfig(name, config);
  }

  @Delete(':name/queue')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Clear pipeline queue' })
  @ApiParam({ name: 'name', description: 'Pipeline name' })
  @ApiResponse({ status: 200, description: 'Queue cleared' })
  async clearQueue(@Param('name') name: string) {
    return this.pipelinesService.clearQueue(name);
  }

  @Get('running/all')
  @ApiOperation({ summary: 'Get all currently running pipelines' })
  @ApiResponse({ status: 200, description: 'List of running pipelines' })
  async getRunningPipelines() {
    return this.pipelinesService.getRunningPipelines();
  }
}