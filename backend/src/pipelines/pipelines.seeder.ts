import { DataSource } from 'typeorm';
import { PipelineState } from './entities/pipeline-state.entity';

const pipelines = [
  'agents',
  'balance_with_bot',
  'balance_with_mnos',
  'balance_with_other_banks',
  'cash',
  'agent_transactions',
  'loans',
  'mobile_banking',
  'outgoing_fund_transfer',
  'pos_transactions',
  'share_capital',
  'personal_data_corporates',
];

export async function seedPipelines(dataSource: DataSource) {
  const repository = dataSource.getRepository(PipelineState);

  for (const pipelineName of pipelines) {
    const existing = await repository.findOne({
      where: { pipelineName },
    });

    if (!existing) {
      const pipeline = new PipelineState();
      pipeline.pipelineName = pipelineName;
      pipeline.lastRun = null as any;
      pipeline.lastSuccessfulRun = null as any;
      pipeline.lastRunStatus = 'idle';
      pipeline.recordsProcessed = 0;
      pipeline.errorMessage = null as any;

      await repository.save(pipeline);
      console.log(`Seeded pipeline: ${pipelineName}`);
    } else {
      console.log(`Pipeline already exists: ${pipelineName}`);
    }
  }
}