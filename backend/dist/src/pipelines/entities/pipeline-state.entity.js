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
Object.defineProperty(exports, "__esModule", { value: true });
exports.PipelineState = void 0;
const typeorm_1 = require("typeorm");
let PipelineState = class PipelineState {
};
exports.PipelineState = PipelineState;
__decorate([
    (0, typeorm_1.PrimaryColumn)({ name: 'pipeline_name', length: 100 }),
    __metadata("design:type", String)
], PipelineState.prototype, "pipelineName", void 0);
__decorate([
    (0, typeorm_1.Column)({ name: 'last_run', type: 'timestamp', nullable: true }),
    __metadata("design:type", Date)
], PipelineState.prototype, "lastRun", void 0);
__decorate([
    (0, typeorm_1.Column)({ name: 'last_successful_run', type: 'timestamp', nullable: true }),
    __metadata("design:type", Date)
], PipelineState.prototype, "lastSuccessfulRun", void 0);
__decorate([
    (0, typeorm_1.Column)({ name: 'last_run_status', length: 20, nullable: true }),
    __metadata("design:type", String)
], PipelineState.prototype, "lastRunStatus", void 0);
__decorate([
    (0, typeorm_1.Column)({ name: 'records_processed', type: 'bigint', default: 0 }),
    __metadata("design:type", Number)
], PipelineState.prototype, "recordsProcessed", void 0);
__decorate([
    (0, typeorm_1.Column)({ name: 'error_message', type: 'text', nullable: true }),
    __metadata("design:type", String)
], PipelineState.prototype, "errorMessage", void 0);
__decorate([
    (0, typeorm_1.CreateDateColumn)({ name: 'created_at' }),
    __metadata("design:type", Date)
], PipelineState.prototype, "createdAt", void 0);
__decorate([
    (0, typeorm_1.UpdateDateColumn)({ name: 'updated_at' }),
    __metadata("design:type", Date)
], PipelineState.prototype, "updatedAt", void 0);
exports.PipelineState = PipelineState = __decorate([
    (0, typeorm_1.Entity)('pipeline_state')
], PipelineState);
//# sourceMappingURL=pipeline-state.entity.js.map