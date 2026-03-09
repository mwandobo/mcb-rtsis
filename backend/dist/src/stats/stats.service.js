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
Object.defineProperty(exports, "__esModule", { value: true });
exports.StatsService = void 0;
const common_1 = require("@nestjs/common");
const typeorm_1 = require("@nestjs/typeorm");
const typeorm_2 = require("typeorm");
const pipeline_state_entity_1 = require("../pipelines/entities/pipeline-state.entity");
let StatsService = class StatsService {
    constructor(pipelineStateRepository) {
        this.pipelineStateRepository = pipelineStateRepository;
    }
    async getDashboardStats() {
        const pipelines = await this.pipelineStateRepository.find();
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const runningPipelines = pipelines.filter((p) => p.lastRunStatus === 'running').length;
        const failedToday = pipelines.filter((p) => p.lastRunStatus === 'failed' && p.lastRun && p.lastRun >= today).length;
        const completedPipelines = pipelines.filter((p) => p.lastRunStatus === 'completed');
        const successRate = completedPipelines.length > 0
            ? (completedPipelines.length / pipelines.length) * 100
            : 0;
        const totalRecordsToday = pipelines.reduce((sum, p) => {
            if (p.lastRun && p.lastRun >= today) {
                return sum + Number(p.recordsProcessed);
            }
            return sum;
        }, 0);
        const pipelinesByStatus = {};
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
    async getRecordsOverTime(days = 7) {
        const data = [];
        const today = new Date();
        for (let i = days - 1; i >= 0; i--) {
            const date = new Date(today);
            date.setDate(date.getDate() - i);
            date.setHours(0, 0, 0, 0);
            const nextDate = new Date(date);
            nextDate.setDate(nextDate.getDate() + 1);
            const pipelines = await this.pipelineStateRepository.find({
                where: {
                    lastRun: (0, typeorm_2.Between)(date, nextDate),
                },
            });
            const totalRecords = pipelines.reduce((sum, p) => sum + Number(p.recordsProcessed), 0);
            data.push({
                timestamp: date.toISOString().split('T')[0],
                records: totalRecords,
                pipelines: pipelines.length,
            });
        }
        return data;
    }
    async getTopPipelines(limit = 10) {
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
    async getRecentFailures(limit = 10) {
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
    async getHealthStatus() {
        const checks = {};
        let hasErrors = false;
        try {
            const pipelines = await this.pipelineStateRepository.find({ take: 1 });
            checks.database = { status: 'healthy', message: 'Connected' };
        }
        catch (error) {
            checks.database = { status: 'critical', message: error.message };
            hasErrors = true;
        }
        const runningCount = (await this.pipelineStateRepository.find({
            where: { lastRunStatus: 'running' },
        })).length;
        checks.runningPipelines = {
            status: 'healthy',
            count: runningCount,
        };
        const status = hasErrors ? 'critical' : 'healthy';
        return { status, checks };
    }
};
exports.StatsService = StatsService;
exports.StatsService = StatsService = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, typeorm_1.InjectRepository)(pipeline_state_entity_1.PipelineState)),
    __metadata("design:paramtypes", [typeorm_2.Repository])
], StatsService);
//# sourceMappingURL=stats.service.js.map