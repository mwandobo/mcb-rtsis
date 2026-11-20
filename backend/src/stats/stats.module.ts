import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { StatsController } from './stats.controller';
import { StatsService } from './stats.service';
import { PipelineState } from '../pipelines/entities/pipeline-state.entity';
import { PipelinesModule } from '../pipelines/pipelines.module';

@Module({
  imports: [TypeOrmModule.forFeature([PipelineState]), PipelinesModule],
  controllers: [StatsController],
  providers: [StatsService],
  exports: [StatsService],
})
export class StatsModule {}