import { Controller, Get, Post, Delete, Param, Query, HttpException, HttpStatus, Logger } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam, ApiQuery } from '@nestjs/swagger';
import { DatabasesService } from './databases.service';

@ApiTags('databases')
@Controller('databases')
export class DatabasesController {
  private readonly logger = new Logger(DatabasesController.name);

  constructor(private readonly databasesService: DatabasesService) {}

  @Get('db2/tables')
  @ApiOperation({ summary: 'Get all DB2 tables with record counts' })
  @ApiResponse({ status: 200, description: 'List of DB2 tables' })
  @ApiResponse({ status: 500, description: 'Error connecting to DB2' })
  async getDb2Tables() {
    this.logger.log('📥 GET /databases/db2/tables - Fetching DB2 tables...');
    try {
      const result = await this.databasesService.getDb2Tables();
      this.logger.log(`📤 DB2 tables response: ${result.length} tables`);
      return result;
    } catch (error: any) {
      this.logger.error(`❌ DB2 tables error: ${error.message}`);
      throw new HttpException(
        { message: 'Failed to connect to DB2', error: error.message },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get('db2/table/:name')
  @ApiOperation({ summary: 'Get data from a DB2 table' })
  @ApiParam({ name: 'name', description: 'Table name' })
  @ApiQuery({ name: 'limit', required: false, type: Number })
  @ApiQuery({ name: 'offset', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Table data' })
  async getDb2TableData(
    @Param('name') name: string,
    @Query('limit') limit?: number,
    @Query('offset') offset?: number,
  ) {
    this.logger.log(`📥 GET /databases/db2/table/${name}`);
    return this.databasesService.getDb2TableData(name, limit || 100, offset || 0);
  }

  @Get('postgres/tables')
  @ApiOperation({ summary: 'Get all PostgreSQL tables with record counts' })
  @ApiResponse({ status: 200, description: 'List of PostgreSQL tables' })
  async getPostgresTables() {
    this.logger.log('📥 GET /databases/postgres/tables - Fetching PostgreSQL tables...');
    try {
      const result = await this.databasesService.getPostgresTables();
      this.logger.log(`📤 PostgreSQL tables response: ${result.length} tables`);
      return result;
    } catch (error: any) {
      this.logger.error(`❌ PostgreSQL tables error: ${error.message}`);
      throw new HttpException(
        { message: 'Failed to connect to PostgreSQL', error: error.message },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get('postgres/table/:name')
  @ApiOperation({ summary: 'Get data from a PostgreSQL table' })
  @ApiParam({ name: 'name', description: 'Table name' })
  @ApiQuery({ name: 'limit', required: false, type: Number })
  @ApiQuery({ name: 'offset', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Table data' })
  async getPostgresTableData(
    @Param('name') name: string,
    @Query('limit') limit?: number,
    @Query('offset') offset?: number,
  ) {
    this.logger.log(`📥 GET /databases/postgres/table/${name}`);
    return this.databasesService.getPostgresTableData(name, limit || 100, offset || 0);
  }

  @Get('postgres/pipeline-stats')
  @ApiOperation({ summary: 'Get pipeline statistics from PostgreSQL' })
  @ApiResponse({ status: 200, description: 'Pipeline statistics' })
  async getPipelineStats() {
    this.logger.log('📥 GET /databases/postgres/pipeline-stats');
    return this.databasesService.getPipelineStats();
  }

  @Delete('db2/cache')
  @ApiOperation({ summary: 'Clear DB2 tables cache' })
  @ApiResponse({ status: 200, description: 'Cache cleared successfully' })
  clearDb2Cache() {
    this.logger.log('📥 DELETE /databases/db2/cache - Clearing cache...');
    this.databasesService.clearDb2Cache();
    return { success: true, message: 'DB2 cache cleared' };
  }

  @Get('db2/cache-info')
  @ApiOperation({ summary: 'Get DB2 cache information' })
  @ApiResponse({ status: 200, description: 'Cache information' })
  getDb2CacheInfo() {
    return this.databasesService.getCacheInfo();
  }
}