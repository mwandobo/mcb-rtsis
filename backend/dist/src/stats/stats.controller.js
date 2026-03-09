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
exports.StatsController = void 0;
const common_1 = require("@nestjs/common");
const swagger_1 = require("@nestjs/swagger");
const stats_service_1 = require("./stats.service");
let StatsController = class StatsController {
    constructor(statsService) {
        this.statsService = statsService;
    }
    async getDashboardStats() {
        return this.statsService.getDashboardStats();
    }
    async getRecordsOverTime(days) {
        return this.statsService.getRecordsOverTime(days || 7);
    }
    async getTopPipelines(limit) {
        return this.statsService.getTopPipelines(limit || 10);
    }
    async getRecentFailures(limit) {
        return this.statsService.getRecentFailures(limit || 10);
    }
    async getHealthStatus() {
        return this.statsService.getHealthStatus();
    }
};
exports.StatsController = StatsController;
__decorate([
    (0, common_1.Get)(),
    (0, swagger_1.ApiOperation)({ summary: 'Get dashboard summary statistics' }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], StatsController.prototype, "getDashboardStats", null);
__decorate([
    (0, common_1.Get)('records-over-time'),
    (0, swagger_1.ApiOperation)({ summary: 'Get records processed over time' }),
    (0, swagger_1.ApiQuery)({ name: 'days', required: false, type: Number }),
    __param(0, (0, common_1.Query)('days')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Number]),
    __metadata("design:returntype", Promise)
], StatsController.prototype, "getRecordsOverTime", null);
__decorate([
    (0, common_1.Get)('top-pipelines'),
    (0, swagger_1.ApiOperation)({ summary: 'Get top pipelines by records processed' }),
    (0, swagger_1.ApiQuery)({ name: 'limit', required: false, type: Number }),
    __param(0, (0, common_1.Query)('limit')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Number]),
    __metadata("design:returntype", Promise)
], StatsController.prototype, "getTopPipelines", null);
__decorate([
    (0, common_1.Get)('recent-failures'),
    (0, swagger_1.ApiOperation)({ summary: 'Get recent pipeline failures' }),
    (0, swagger_1.ApiQuery)({ name: 'limit', required: false, type: Number }),
    __param(0, (0, common_1.Query)('limit')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Number]),
    __metadata("design:returntype", Promise)
], StatsController.prototype, "getRecentFailures", null);
__decorate([
    (0, common_1.Get)('health'),
    (0, swagger_1.ApiOperation)({ summary: 'Get system health status' }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], StatsController.prototype, "getHealthStatus", null);
exports.StatsController = StatsController = __decorate([
    (0, swagger_1.ApiTags)('stats'),
    (0, common_1.Controller)('stats'),
    __metadata("design:paramtypes", [stats_service_1.StatsService])
], StatsController);
//# sourceMappingURL=stats.controller.js.map