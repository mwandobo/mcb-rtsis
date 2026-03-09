import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { PipelinesController } from './pipelines.controller';
import { PipelinesService } from './pipelines.service';
import { PipelineState } from './entities/pipeline-state.entity';

@Module({
  imports: [TypeOrmModule.forFeature([PipelineState])],
  controllers: [PipelinesController],
  providers: [PipelinesService],
  exports: [PipelinesService],
})
export class PipelinesModule {}