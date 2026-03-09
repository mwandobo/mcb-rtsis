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
exports.AppModule = void 0;
const common_1 = require("@nestjs/common");
const typeorm_1 = require("@nestjs/typeorm");
const config_module_1 = require("./config/config.module");
const config_service_1 = require("./config/config.service");
const pipelines_module_1 = require("./pipelines/pipelines.module");
const stats_module_1 = require("./stats/stats.module");
const databases_module_1 = require("./databases/databases.module");
const pipeline_state_entity_1 = require("./pipelines/entities/pipeline-state.entity");
const typeorm_2 = require("typeorm");
const pipelines_seeder_1 = require("./pipelines/pipelines.seeder");
let AppModule = class AppModule {
    constructor(dataSource) {
        this.dataSource = dataSource;
    }
    async onModuleInit() {
        console.log('Seeding pipelines...');
        await (0, pipelines_seeder_1.seedPipelines)(this.dataSource);
        console.log('Pipeline seeding complete');
    }
};
exports.AppModule = AppModule;
exports.AppModule = AppModule = __decorate([
    (0, common_1.Module)({
        imports: [
            config_module_1.ConfigModule,
            typeorm_1.TypeOrmModule.forRootAsync({
                imports: [config_module_1.ConfigModule],
                useFactory: (configService) => {
                    const pgConfig = configService.getPgConfig();
                    return {
                        type: 'postgres',
                        host: pgConfig.host,
                        port: pgConfig.port,
                        username: pgConfig.username,
                        password: pgConfig.password,
                        database: pgConfig.database,
                        entities: [pipeline_state_entity_1.PipelineState],
                        synchronize: false,
                        logging: configService.get('NODE_ENV') === 'development',
                    };
                },
                inject: [config_service_1.ConfigService],
            }),
            pipelines_module_1.PipelinesModule,
            stats_module_1.StatsModule,
            databases_module_1.DatabasesModule,
        ],
    }),
    __metadata("design:paramtypes", [typeorm_2.DataSource])
], AppModule);
//# sourceMappingURL=app.module.js.map