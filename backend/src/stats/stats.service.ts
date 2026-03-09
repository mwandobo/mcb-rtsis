import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between } from 'typeorm';
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

@Injectable()
export class StatsService {
  constructor(
    @InjectRepository(PipelineState)
    private readonly pipelineStateRepository: Repository<PipelineState>,
  ) {}

  async getDashboardStats(): Promise<DashboardStats> {
    const pipelines = await this.pipelineStateRepository.find();

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const runningPipelines = pipelines.filter((p) => p.lastRunStatus === 'running').length;
    const failedToday = pipelines.filter(
      (p) => p.lastRunStatus === 'failed' && p.lastRun && p.lastRun >= today,
    ).length;

    const completedPipelines = pipelines.filter((p) => p.lastRunStatus === 'completed');
    const successRate =
      completedPipelines.length > 0
        ? (completedPipelines.length / pipelines.length) * 100
        : 0;

    const totalRecordsToday = pipelines.reduce((sum, p) => {
      if (p.lastRun && p.lastRun >= today) {
        return sum + Number(p.recordsProcessed);
      }
      return sum;
    }, 0);

    const pipelinesByStatus: Record<string, number> = {};
    pipelines.forEach((p) => {
      const status = p.lastRunStatus || 'unknown';
      pipelinesByStatus[status] = (pipelinesByStatus[status] || 0) + 1;
    });

    return {
      totalPipelines: pipelines.length,
      runningPipelines,
      failedToday,
      successRate: Math.round(successRate * 10) / 10,
      totalRecordsToday,
      pipelinesByStatus,
    };
  }

  async getRecordsOverTime(days = 7): Promise<TimeSeriesData[]> {
    const data: TimeSeriesData[] = [];
    const today = new Date();

    for (let i = days - 1; i >= 0; i--) {
      const date = new Date(today);
      date.setDate(date.getDate() - i);
      date.setHours(0, 0, 0, 0);

      const nextDate = new Date(date);
      nextDate.setDate(nextDate.getDate() + 1);

      const pipelines = await this.pipelineStateRepository.find({
        where: {
          lastRun: Between(date, nextDate),
        },
      });

      const totalRecords = pipelines.reduce(
        (sum, p) => sum + Number(p.recordsProcessed),
        0,
      );

      data.push({
        timestamp: date.toISOString().split('T')[0],
        records: totalRecords,
        pipelines: pipelines.length,
      });
    }

    return data;
  }

  async getTopPipelines(limit = 10): Promise<any[]> {
    const pipelines = await this.pipelineStateRepository.find({
      order: { recordsProcessed: 'DESC' },
      take: limit,
    });

    return pipelines.map((p) => ({
      name: p.pipelineName,
      records: p.recordsProcessed,
      lastRun: p.lastRun,
      status: p.lastRunStatus,
    }));
  }

  async getRecentFailures(limit = 10): Promise<any[]> {
    const pipelines = await this.pipelineStateRepository.find({
      where: { lastRunStatus: 'failed' },
      order: { lastRun: 'DESC' },
      take: limit,
    });

    return pipelines.map((p) => ({
      name: p.pipelineName,
      lastRun: p.lastRun,
      errorMessage: p.errorMessage,
    }));
  }

  async getHealthStatus(): Promise<{
    status: 'healthy' | 'warning' | 'critical';
    checks: Record<string, any>;
  }> {
    const checks: Record<string, any> = {};
    let hasErrors = false;

    // Check database connection
    try {
      const pipelines = await this.pipelineStateRepository.find({ take: 1 });
      checks.database = { status: 'healthy', message: 'Connected' };
    } catch (error) {
      checks.database = { status: 'critical', message: error.message };
      hasErrors = true;
    }

    // Check running pipelines
    const runningCount = (
      await this.pipelineStateRepository.find({
        where: { lastRunStatus: 'running' },
      })
    ).length;
    checks.runningPipelines = {
      status: 'healthy',
      count: runningCount,
    };

    // Overall status
    const status = hasErrors ? 'critical' : 'healthy';

    return { status, checks };
  }
}