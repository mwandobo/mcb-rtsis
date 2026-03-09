import { Entity, Column, PrimaryColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('pipeline_state')
export class PipelineState {
  @PrimaryColumn({ name: 'pipeline_name', length: 100 })
  pipelineName: string;

  @Column({ name: 'last_run', type: 'timestamp', nullable: true })
  lastRun: Date;

  @Column({ name: 'last_successful_run', type: 'timestamp', nullable: true })
  lastSuccessfulRun: Date;

  @Column({ name: 'last_run_status', length: 20, nullable: true })
  lastRunStatus: string;

  @Column({ name: 'records_processed', type: 'bigint', default: 0 })
  recordsProcessed: number;

  @Column({ name: 'error_message', type: 'text', nullable: true })
  errorMessage: string;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}