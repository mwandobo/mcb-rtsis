import { Injectable, NotFoundException, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { spawn, ChildProcess } from 'child_process';
import { join } from 'path';
import * as amqp from 'amqplib';
import { PipelineState } from './entities/pipeline-state.entity';
import { ConfigService } from '../config/config.service';

// Pipelines pinned to the front of the list, in display order, as they're
// migrated to incremental mode (matches run_all_pipelines.py's order).
const PINNED_ORDER = ['branch', 'agents', 'pos', 'atm', 'personal_data_information', 'personal_data_corporates', 'employee', 'directors_records', 'account_product_category', 'account_information'];

export interface RunPipelineDto {
  fullLoad?: boolean;
}

export interface PipelineRunResult {
  success: boolean;
  pipelineName: string;
  processId?: number;
  message: string;
}

@Injectable()
export class PipelinesService {
  private readonly logger = new Logger(PipelinesService.name);
  private runningProcesses: Map<string, ChildProcess> = new Map();

  constructor(
    @InjectRepository(PipelineState)
    private readonly pipelineStateRepository: Repository<PipelineState>,
    private readonly configService: ConfigService,
  ) {}

  async findAll(): Promise<PipelineState[]> {
    const pipelines = await this.pipelineStateRepository.find({
      order: { pipelineName: 'ASC' },
    });

    // Pipelines are pinned in the same order they run in run_all_pipelines.py
    // (branch first as foundational master data, agents next, etc.); anything
    // not yet pinned falls back after them in alphabetical order.
    pipelines.sort((a, b) => {
      const indexA = PINNED_ORDER.indexOf(a.pipelineName);
      const indexB = PINNED_ORDER.indexOf(b.pipelineName);
      if (indexA !== -1 && indexB !== -1) return indexA - indexB;
      if (indexA !== -1) return -1;
      if (indexB !== -1) return 1;
      return 0;
    });

    return pipelines;
  }

  async findOne(name: string): Promise<PipelineState> {
    const pipeline = await this.pipelineStateRepository.findOne({
      where: { pipelineName: name },
    });

    if (!pipeline) {
      throw new NotFoundException(`Pipeline '${name}' not found`);
    }

    return pipeline;
  }

  async getHistory(name: string, limit = 20): Promise<any[]> {
    // For now, return the current state
    // In production, you'd have a separate history table
    const pipeline = await this.findOne(name);
    return [
      {
        runTime: pipeline.lastRun,
        status: pipeline.lastRunStatus,
        records: pipeline.recordsProcessed,
        errorMessage: pipeline.errorMessage,
      },
    ];
  }

  async runPipeline(name: string, options: RunPipelineDto = {}): Promise<PipelineRunResult> {
    const pipeline = await this.findOne(name);

    // Check if pipeline is already running
    if (this.runningProcesses.has(name)) {
      const existingProcess = this.runningProcesses.get(name);
      if (existingProcess && !existingProcess.killed) {
        return {
          success: false,
          pipelineName: name,
          processId: -1,
          message: 'Pipeline is already running',
        };
      }
    }

    const pipelinesDir = this.configService.get<string>('PIPELINES_DIR');
    const scriptPath = join(pipelinesDir, 'run_continuous.py');

    // Check if the continuous runner exists
    const fs = require('fs');
    if (!fs.existsSync(scriptPath)) {
      this.logger.error(`Continuous runner not found: ${scriptPath}`);
      return {
        success: false,
        pipelineName: name,
        message: `Continuous runner not found: ${scriptPath}`,
      };
    }

    const args: string[] = [name];
    if (options.fullLoad) {
      args.push('--full-load');
    }

    this.logger.log(`Starting pipeline (continuous): ${name}`);
    this.logger.log(`Script path: ${scriptPath}`);
    this.logger.log(`Args: ${args.join(' ')}`);

    // This process is expected to keep running indefinitely - it stays
    // alive until it errors out on its own or is stopped via stopPipeline().
    const childProcess = spawn('python', [scriptPath, ...args], {
      cwd: pipelinesDir,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, PYTHONUNBUFFERED: '1' },
    });

    let stdout = '';
    let stderr = '';
    let startTime = Date.now();

    childProcess.stdout.on('data', (data) => {
      const text = data.toString();
      stdout += text;
      this.logger.log(`[${name}] ${text.trim()}`);
    });

    childProcess.stderr.on('data', (data) => {
      const text = data.toString();
      stderr += text;
      this.logger.error(`[${name}] ${text.trim()}`);
    });

    childProcess.on('close', (code) => {
      const duration = (Date.now() - startTime) / 1000;
      this.logger.log(`[${name}] Process exited with code ${code} after ${duration}s`);
      this.logger.log(`[${name}] stdout length: ${stdout.length}, stderr length: ${stderr.length}`);
      this.runningProcesses.delete(name);
    });

    childProcess.on('error', (error) => {
      this.logger.error(`[${name}] Process error: ${error.message}`);
      this.runningProcesses.delete(name);
    });

    this.runningProcesses.set(name, childProcess);

    return {
      success: true,
      pipelineName: name,
      processId: childProcess.pid,
      message: `Pipeline started (PID: ${childProcess.pid}). Check logs for progress.`,
    };
  }

  async stopPipeline(name: string): Promise<{ success: boolean; message: string }> {
    const process = this.runningProcesses.get(name);

    if (!process || process.killed) {
      return {
        success: false,
        message: 'Pipeline is not currently running',
      };
    }

    process.kill('SIGTERM');
    this.runningProcesses.delete(name);

    return {
      success: true,
      message: `Pipeline '${name}' stopped successfully`,
    };
  }

  async getRunningPipelines(): Promise<string[]> {
    const running: string[] = [];

    // In-memory tracked processes are the only source of truth for "running" -
    // this backend never resumes a process after restart, so a DB row stuck on
    // 'running' always means the process died mid-cycle without reaching its
    // own completed/failed update, never a genuinely live pipeline we lost
    // track of. Trusting that DB flag used to make such a pipeline show as
    // running forever on the dashboard while stopPipeline() (which only
    // checks runningProcesses) correctly refused to stop a process that no
    // longer exists. Self-heal those stuck rows instead of trusting them.
    this.runningProcesses.forEach((process, name) => {
      if (!process.killed) {
        running.push(name);
      }
    });

    try {
      const stuck = await this.pipelineStateRepository
        .createQueryBuilder('ps')
        .select('ps.pipelineName', 'pipelineName')
        .where('ps.lastRunStatus = :status', { status: 'running' })
        .getRawMany();

      for (const row of stuck) {
        if (!running.includes(row.pipelineName)) {
          await this.pipelineStateRepository.update(
            { pipelineName: row.pipelineName },
            {
              lastRunStatus: 'failed',
              errorMessage: 'Process lost (backend lost track of it, likely a crash) - start it again',
            },
          );
        }
      }
    } catch (err) {
      this.logger.error('Error reconciling stale running state:', err);
    }

    return running;
  }

  async getPipelineConfig(name: string): Promise<any> {
    // Return basic config info
    // In production, you'd read from a config file
    return {
      pipelineName: name,
      mode: 'incremental',
      schedule: '*/5 * * * *',
      source: 'GLI_TRX_EXTRACT.TMSTAMP',
      queue: `${name}_queue`,
    };
  }

  async updateConfig(name: string, config: any): Promise<{ success: boolean; message: string }> {
    // In production, you'd update a config file
    return {
      success: true,
      message: `Configuration updated for pipeline '${name}'`,
    };
  }

  async clearQueue(name: string): Promise<{ success: boolean; message: string }> {
    try {
      const rabbitmqHost = this.configService.get<string>('RABBITMQ_HOST') || 'localhost';
      const rabbitmqPort = parseInt(this.configService.get<string>('RABBITMQ_PORT') || '5672');
      const rabbitmqUser = this.configService.get<string>('RABBITMQ_USER') || 'guest';
      const rabbitmqPassword = this.configService.get<string>('RABBITMQ_PASSWORD') || 'guest';

      const queueName = `${name}_queue`;
      
      const connection = await amqp.connect({
        hostname: rabbitmqHost,
        port: rabbitmqPort,
        username: rabbitmqUser,
        password: rabbitmqPassword,
      });

      const channel = await connection.createChannel();
      
      // Purge the queue (remove all messages)
      const result = await channel.purgeQueue(queueName);
      
      await channel.close();
      await connection.close();

      this.logger.log(`Queue '${queueName}' purged, removed ${result.messageCount} messages`);

      return {
        success: true,
        message: `Queue '${queueName}' cleared (${result.messageCount} messages removed)`,
      };
    } catch (error: any) {
      this.logger.error(`Failed to clear queue: ${error.message}`);
      return {
        success: false,
        message: `Failed to clear queue: ${error.message}`,
      };
    }
  }
}