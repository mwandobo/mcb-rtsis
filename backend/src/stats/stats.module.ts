import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { StatsController } from './stats.controller';
import { StatsService } from './stats.service';
import { PipelineState } from '../pipelines/entities/pipeline-state.entity';

@Module({
  imports: [TypeOrmModule.forFeature([PipelineState])],
  controllers: [StatsController],
  providers: [StatsService],
  exports: [StatsService],
})
export class StatsModule {}