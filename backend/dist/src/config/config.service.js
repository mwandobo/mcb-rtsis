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
exports.ConfigService = void 0;
const common_1 = require("@nestjs/common");
const dotenv = require("dotenv");
let ConfigService = class ConfigService {
    constructor() {
        dotenv.config();
        this.config = {
            DB_HOST: process.env.DB2_HOST || 'localhost',
            DB_PORT: process.env.DB2_PORT || '50000',
            DB_USERNAME: process.env.DB2_USER || 'db2inst1',
            DB_PASSWORD: process.env.DB2_PASSWORD || 'password',
            DB_NAME: process.env.DB2_DATABASE || 'bankdb',
            PG_HOST: process.env.PG_HOST || 'localhost',
            PG_PORT: process.env.PG_PORT || '5432',
            PG_USER: process.env.PG_USER || 'postgres',
            PG_PASSWORD: process.env.PG_PASSWORD || 'postgres',
            PG_DATABASE: process.env.PG_DATABASE || 'bankdb',
            RABBITMQ_HOST: process.env.RABBITMQ_HOST || 'localhost',
            RABBITMQ_PORT: process.env.RABBITMQ_PORT || '5672',
            RABBITMQ_USER: process.env.RABBITMQ_USER || 'guest',
            RABBITMQ_PASSWORD: process.env.RABBITMQ_PASSWORD || 'guest',
            PORT: process.env.PORT || '3000',
            NODE_ENV: process.env.NODE_ENV || 'development',
            PIPELINES_DIR: process.env.PIPELINES_DIR || '../',
        };
    }
    get(key) {
        return this.config[key];
    }
    getDatabaseConfig() {
        return {
            host: this.config.DB_HOST,
            port: parseInt(this.config.DB_PORT, 10),
            username: this.config.DB_USERNAME,
            password: this.config.DB_PASSWORD,
            database: this.config.DB_NAME,
        };
    }
    getPgConfig() {
        return {
            host: this.config.PG_HOST,
            port: parseInt(this.config.PG_PORT, 10),
            username: this.config.PG_USER,
            password: this.config.PG_PASSWORD,
            database: this.config.PG_DATABASE,
        };
    }
    getRabbitMQConfig() {
        return {
            host: this.config.RABBITMQ_HOST,
            port: parseInt(this.config.RABBITMQ_PORT, 10),
            username: this.config.RABBITMQ_USER,
            password: this.config.RABBITMQ_PASSWORD,
        };
    }
};
exports.ConfigService = ConfigService;
exports.ConfigService = ConfigService = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [])
], ConfigService);
//# sourceMappingURL=config.service.js.map