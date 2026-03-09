import { Controller, Get, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiQuery } from '@nestjs/swagger';
import { StatsService } from './stats.service';

@ApiTags('stats')
@Controller('stats')
export class StatsController {
  constructor(private readonly statsService: StatsService) {}

  @Get()
  @ApiOperation({ summary: 'Get dashboard summary statistics' })
  async getDashboardStats() {
    return this.statsService.getDashboardStats();
  }

  @Get('records-over-time')
  @ApiOperation({ summary: 'Get records processed over time' })
  @ApiQuery({ name: 'days', required: false, type: Number })
  async getRecordsOverTime(@Query('days') days?: number) {
    return this.statsService.getRecordsOverTime(days || 7);
  }

  @Get('top-pipelines')
  @ApiOperation({ summary: 'Get top pipelines by records processed' })
  @ApiQuery({ name: 'limit', required: false, type: Number })
  async getTopPipelines(@Query('limit') limit?: number) {
    return this.statsService.getTopPipelines(limit || 10);
  }

  @Get('recent-failures')
  @ApiOperation({ summary: 'Get recent pipeline failures' })
  @ApiQuery({ name: 'limit', required: false, type: Number })
  async getRecentFailures(@Query('limit') limit?: number) {
    return this.statsService.getRecentFailures(limit || 10);
  }

  @Get('health')
  @ApiOperation({ summary: 'Get system health status' })
  async getHealthStatus() {
    return this.statsService.getHealthStatus();
  }
}