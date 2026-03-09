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
var DatabasesController_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.DatabasesController = void 0;
const common_1 = require("@nestjs/common");
const swagger_1 = require("@nestjs/swagger");
const databases_service_1 = require("./databases.service");
let DatabasesController = DatabasesController_1 = class DatabasesController {
    constructor(databasesService) {
        this.databasesService = databasesService;
        this.logger = new common_1.Logger(DatabasesController_1.name);
    }
    async getDb2Tables() {
        this.logger.log('📥 GET /databases/db2/tables - Fetching DB2 tables...');
        try {
            const result = await this.databasesService.getDb2Tables();
            this.logger.log(`📤 DB2 tables response: ${result.length} tables`);
            return result;
        }
        catch (error) {
            this.logger.error(`❌ DB2 tables error: ${error.message}`);
            throw new common_1.HttpException({ message: 'Failed to connect to DB2', error: error.message }, common_1.HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    async getDb2TableData(name, limit, offset) {
        this.logger.log(`📥 GET /databases/db2/table/${name}`);
        return this.databasesService.getDb2TableData(name, limit || 100, offset || 0);
    }
    async getPostgresTables() {
        this.logger.log('📥 GET /databases/postgres/tables - Fetching PostgreSQL tables...');
        try {
            const result = await this.databasesService.getPostgresTables();
            this.logger.log(`📤 PostgreSQL tables response: ${result.length} tables`);
            return result;
        }
        catch (error) {
            this.logger.error(`❌ PostgreSQL tables error: ${error.message}`);
            throw new common_1.HttpException({ message: 'Failed to connect to PostgreSQL', error: error.message }, common_1.HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    async getPostgresTableData(name, limit, offset) {
        this.logger.log(`📥 GET /databases/postgres/table/${name}`);
        return this.databasesService.getPostgresTableData(name, limit || 100, offset || 0);
    }
    async getPipelineStats() {
        this.logger.log('📥 GET /databases/postgres/pipeline-stats');
        return this.databasesService.getPipelineStats();
    }
};
exports.DatabasesController = DatabasesController;
__decorate([
    (0, common_1.Get)('db2/tables'),
    (0, swagger_1.ApiOperation)({ summary: 'Get all DB2 tables with record counts' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'List of DB2 tables' }),
    (0, swagger_1.ApiResponse)({ status: 500, description: 'Error connecting to DB2' }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], DatabasesController.prototype, "getDb2Tables", null);
__decorate([
    (0, common_1.Get)('db2/table/:name'),
    (0, swagger_1.ApiOperation)({ summary: 'Get data from a DB2 table' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Table name' }),
    (0, swagger_1.ApiQuery)({ name: 'limit', required: false, type: Number }),
    (0, swagger_1.ApiQuery)({ name: 'offset', required: false, type: Number }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Table data' }),
    __param(0, (0, common_1.Param)('name')),
    __param(1, (0, common_1.Query)('limit')),
    __param(2, (0, common_1.Query)('offset')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, Number]),
    __metadata("design:returntype", Promise)
], DatabasesController.prototype, "getDb2TableData", null);
__decorate([
    (0, common_1.Get)('postgres/tables'),
    (0, swagger_1.ApiOperation)({ summary: 'Get all PostgreSQL tables with record counts' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'List of PostgreSQL tables' }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], DatabasesController.prototype, "getPostgresTables", null);
__decorate([
    (0, common_1.Get)('postgres/table/:name'),
    (0, swagger_1.ApiOperation)({ summary: 'Get data from a PostgreSQL table' }),
    (0, swagger_1.ApiParam)({ name: 'name', description: 'Table name' }),
    (0, swagger_1.ApiQuery)({ name: 'limit', required: false, type: Number }),
    (0, swagger_1.ApiQuery)({ name: 'offset', required: false, type: Number }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Table data' }),
    __param(0, (0, common_1.Param)('name')),
    __param(1, (0, common_1.Query)('limit')),
    __param(2, (0, common_1.Query)('offset')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, Number]),
    __metadata("design:returntype", Promise)
], DatabasesController.prototype, "getPostgresTableData", null);
__decorate([
    (0, common_1.Get)('postgres/pipeline-stats'),
    (0, swagger_1.ApiOperation)({ summary: 'Get pipeline statistics from PostgreSQL' }),
    (0, swagger_1.ApiResponse)({ status: 200, description: 'Pipeline statistics' }),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], DatabasesController.prototype, "getPipelineStats", null);
exports.DatabasesController = DatabasesController = DatabasesController_1 = __decorate([
    (0, swagger_1.ApiTags)('databases'),
    (0, common_1.Controller)('databases'),
    __metadata("design:paramtypes", [databases_service_1.DatabasesService])
], DatabasesController);
//# sourceMappingURL=databases.controller.js.map