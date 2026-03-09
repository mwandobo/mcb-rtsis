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
exports.PipelinesController = void 0;
const common_1 = require("@nestjs/common");
const swagger_1 = require("@nestjs/swagger");
const pipelines_service_1 = require("./pipelines.service");
let PipelinesController = class PipelinesController {
    constructor(pipelinesService) {
        this.pipelinesService = pipelinesService;
    }
    async findAll() {
        return this.pipelinesService.findAll();
    }
    async findOne(name) {
        return this.pipelinesService.findOne(name);
    }
    async getHistory(name) {
        return this.pipelinesService.getHistory(name);
    }
    async getConfig(name) {
        return this.pipelinesService.getPipelineConfig(name);
    }
    async runPipeline(name, dto) {
        return this.pipelinesService.runPipeline(name, dto);
    }
    async stopPipeline(name) {
        return this.pipelinesService.stopPipeline(name);
    }
    async updateConfig(name, config) {
        return this.pipelinesService.updateConfig(name, config);
    }
    async clearQueue(name) {
        return this.pipelinesService.clearQueue(name);
    }
    async getRunningPipelines() {
        return this.pipelinesService.getRunningPipelines();
    }
};
exports.PipelinesController = PipelinesController;
__decorate([
    (0, common_1.Get)(),
    (0, swagger_1.ApiOperation)({ summary: 'Get all pipelines with their status' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'List of all pipelines' }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "findAll", null);
__decorate([
    (0, common_1.Get)(':name'),
    (0, swagger_1.ApiOperation)({ summary: 'Get a specific pipeline status' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Pipeline name' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Pipeline details' }),
    (0, swagger_1.ApiResponse)({ status: 404, description: 'Pipeline not found' }),
    __param(0, (0, common_1.Param)('name')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "findOne", null);
__decorate([
    (0, common_1.Get)(':name/history'),
    (0, swagger_1.ApiOperation)({ summary: 'Get pipeline run history' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Pipeline name' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Pipeline run history' }),
    __param(0, (0, common_1.Param)('name')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "getHistory", null);
__decorate([
    (0, common_1.Get)(':name/config'),
    (0, swagger_1.ApiOperation)({ summary: 'Get pipeline configuration' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Pipeline name' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Pipeline configuration' }),
    __param(0, (0, common_1.Param)('name')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "getConfig", null);
__decorate([
    (0, common_1.Post)(':name/run'),
    (0, common_1.HttpCode)(common_1.HttpStatus.OK),
    (0, swagger_1.ApiOperation)({ summary: 'Trigger a pipeline run' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Pipeline name' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Pipeline started' }),
    (0, swagger_1.ApiResponse)({ status: 400, description: 'Pipeline already running' }),
    __param(0, (0, common_1.Param)('name')),
    __param(1, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "runPipeline", null);
__decorate([
    (0, common_1.Post)(':name/stop'),
    (0, common_1.HttpCode)(common_1.HttpStatus.OK),
    (0, swagger_1.ApiOperation)({ summary: 'Stop a running pipeline' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Pipeline name' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Pipeline stopped' }),
    __param(0, (0, common_1.Param)('name')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "stopPipeline", null);
__decorate([
    (0, common_1.Put)(':name/config'),
    (0, swagger_1.ApiOperation)({ summary: 'Update pipeline configuration' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Pipeline name' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Configuration updated' }),
    __param(0, (0, common_1.Param)('name')),
    __param(1, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "updateConfig", null);
__decorate([
    (0, common_1.Delete)(':name/queue'),
    (0, common_1.HttpCode)(common_1.HttpStatus.OK),
    (0, swagger_1.ApiOperation)({ summary: 'Clear pipeline queue' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Pipeline name' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Queue cleared' }),
    __param(0, (0, common_1.Param)('name')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "clearQueue", null);
__decorate([
    (0, common_1.Get)('running/all'),
    (0, swagger_1.ApiOperation)({ summary: 'Get all currently running pipelines' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'List of running pipelines' }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], PipelinesController.prototype, "getRunningPipelines", null);
exports.PipelinesController = PipelinesController = __decorate([
    (0, swagger_1.ApiTags)('pipelines'),
    (0, common_1.Controller)('pipelines'),
    __metadata("design:paramtypes", [pipelines_service_1.PipelinesService])
], PipelinesController);
//# sourceMappingURL=pipelines.controller.js.map