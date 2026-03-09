"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.seedPipelines = seedPipelines;
const pipeline_state_entity_1 = require("./entities/pipeline-state.entity");
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
async function seedPipelines(dataSource) {
    const repository = dataSource.getRepository(pipeline_state_entity_1.PipelineState);
    for (const pipelineName of pipelines) {
        const existing = await repository.findOne({
            where: { pipelineName },
        });
        if (!existing) {
            const pipeline = new pipeline_state_entity_1.PipelineState();
            pipeline.pipelineName = pipelineName;
            pipeline.lastRun = null;
            pipeline.lastSuccessfulRun = null;
            pipeline.lastRunStatus = 'idle';
            pipeline.recordsProcessed = 0;
            pipeline.errorMessage = null;
            await repository.save(pipeline);
            console.log(`Seeded pipeline: ${pipelineName}`);
        }
        else {
            console.log(`Pipeline already exists: ${pipelineName}`);
        }
    }
}
//# sourceMappingURL=pipelines.seeder.js.map