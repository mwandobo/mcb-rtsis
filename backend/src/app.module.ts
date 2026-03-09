import { Module, OnModuleInit } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule } from './config/config.module';
import { ConfigService } from './config/config.service';
import { PipelinesModule } from './pipelines/pipelines.module';
import { StatsModule } from './stats/stats.module';
import { DatabasesModule } from './databases/databases.module';
import { PipelineState } from './pipelines/entities/pipeline-state.entity';
import { DataSource } from 'typeorm';
import { seedPipelines } from './pipelines/pipelines.seeder';

@Module({
  imports: [
    ConfigModule,
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => {
        const pgConfig = configService.getPgConfig();
        return {
          type: 'postgres',
          host: pgConfig.host,
          port: pgConfig.port,
          username: pgConfig.username,
          password: pgConfig.password,
          database: pgConfig.database,
          entities: [PipelineState],
          synchronize: false, // Don't auto-sync, we have existing table
          logging: configService.get<string>('NODE_ENV') === 'development',
        };
      },
      inject: [ConfigService],
    }),
    PipelinesModule,
    StatsModule,
    DatabasesModule,
  ],
})
export class AppModule implements OnModuleInit {
  constructor(private dataSource: DataSource) {}

  async onModuleInit() {
    console.log('Seeding pipelines...');
    await seedPipelines(this.dataSource);
    console.log('Pipeline seeding complete');
  }
}