"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
var PipelinesService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.PipelinesService = void 0;
const common_1 = require("@nestjs/common");
const typeorm_1 = require("@nestjs/typeorm");
const typeorm_2 = require("typeorm");
const child_process_1 = require("child_process");
const path_1 = require("path");
const amqp = require("amqplib");
const pipeline_state_entity_1 = require("./entities/pipeline-state.entity");
const config_service_1 = require("../config/config.service");
let PipelinesService = PipelinesService_1 = class PipelinesService {
    constructor(pipelineStateRepository, configService) {
        this.pipelineStateRepository = pipelineStateRepository;
        this.configService = configService;
        this.logger = new common_1.Logger(PipelinesService_1.name);
        this.runningProcesses = new Map();
    }
    async findAll() {
        return this.pipelineStateRepository.find({
            order: { pipelineName: 'ASC' },
        });
    }
    async findOne(name) {
        const pipeline = await this.pipelineStateRepository.findOne({
            where: { pipelineName: name },
        });
        if (!pipeline) {
            throw new common_1.NotFoundException(`Pipeline '${name}' not found`);
        }
        return pipeline;
    }
    async getHistory(name, limit = 20) {
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
    async runPipeline(name, options = {}) {
        const pipeline = await this.findOne(name);
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
        const pipelinesDir = this.configService.get('PIPELINES_DIR');
        const scriptPath = (0, path_1.join)(pipelinesDir, name, `${name}_streaming_pipeline.py`);
        const fs = require('fs');
        if (!fs.existsSync(scriptPath)) {
            this.logger.error(`Pipeline script not found: ${scriptPath}`);
            return {
                success: false,
                pipelineName: name,
                message: `Pipeline script not found: ${scriptPath}`,
            };
        }
        const args = ['--full-load'];
        if (options.fullLoad) {
            args.push('--full-load');
        }
        this.logger.log(`Starting pipeline: ${name}`);
        this.logger.log(`Script path: ${scriptPath}`);
        this.logger.log(`Args: ${args.join(' ')}`);
        const childProcess = (0, child_process_1.spawn)('python', [scriptPath, ...args], {
            cwd: (0, path_1.join)(pipelinesDir, name),
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
    async stopPipeline(name) {
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
    async getRunningPipelines() {
        const running = [];
        this.runningProcesses.forEach((process, name) => {
            if (!process.killed) {
                running.push(name);
            }
        });
        try {
            const dbRunning = await this.pipelineStateRepository
                .createQueryBuilder('ps')
                .select('ps.pipelineName', 'pipelineName')
                .where('ps.lastRunStatus = :status', { status: 'running' })
                .getRawMany();
            for (const row of dbRunning) {
                if (!running.includes(row.pipelineName)) {
                    running.push(row.pipelineName);
                }
            }
        }
        catch (err) {
            this.logger.error('Error checking database for running pipelines:', err);
        }
        return running;
    }
    async getPipelineConfig(name) {
        return {
            pipelineName: name,
            mode: 'incremental',
            schedule: '*/5 * * * *',
            source: 'GLI_TRX_EXTRACT.TMSTAMP',
            queue: `${name}_queue`,
        };
    }
    async updateConfig(name, config) {
        return {
            success: true,
            message: `Configuration updated for pipeline '${name}'`,
        };
    }
    async clearQueue(name) {
        try {
            const rabbitmqHost = this.configService.get('RABBITMQ_HOST') || 'localhost';
            const rabbitmqPort = parseInt(this.configService.get('RABBITMQ_PORT') || '5672');
            const rabbitmqUser = this.configService.get('RABBITMQ_USER') || 'guest';
            const rabbitmqPassword = this.configService.get('RABBITMQ_PASSWORD') || 'guest';
            const queueName = `${name}_queue`;
            const connection = await amqp.connect({
                hostname: rabbitmqHost,
                port: rabbitmqPort,
                username: rabbitmqUser,
                password: rabbitmqPassword,
            });
            const channel = await connection.createChannel();
            const result = await channel.purgeQueue(queueName);
            await channel.close();
            await connection.close();
            this.logger.log(`Queue '${queueName}' purged, removed ${result.messageCount} messages`);
            return {
                success: true,
                message: `Queue '${queueName}' cleared (${result.messageCount} messages removed)`,
            };
        }
        catch (error) {
            this.logger.error(`Failed to clear queue: ${error.message}`);
            return {
                success: false,
                message: `Failed to clear queue: ${error.message}`,
            };
        }
    }
};
exports.PipelinesService = PipelinesService;
exports.PipelinesService = PipelinesService = PipelinesService_1 = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, typeorm_1.InjectRepository)(pipeline_state_entity_1.PipelineState)),
    __metadata("design:paramtypes", [typeorm_2.Repository,
        config_service_1.ConfigService])
], PipelinesService);
//# sourceMappingURL=pipelines.service.js.map