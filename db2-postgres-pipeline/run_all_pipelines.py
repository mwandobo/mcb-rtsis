#!/usr/bin/env python3
"""
Pipeline Orchestrator - Runs all pipelines sequentially with state management
"""

import sys
import os
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from pipeline_state import PipelineStateManager

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates all pipeline runs sequentially"""
    
    # All available pipelines in execution order
    PIPELINES = [
        {'name':'branch', 'module':'branch','class':'BranchStreamingPipeline'},
        {'name':'agents','module':'agents','class':'AgentsStreamingPipeline'},
        {'name':'pos','module':'pos','class':'PosStreamingPipeline'},
        {'name':'atm','module':'atm','class':'AtmStreamingPipeline'},
        {'name':'personal_data', 'module': 'personal_data', 'class': 'PersonalDataStreamingPipeline'},
        {'name':'personal_data_corporates', 'module': 'personal_data_corporates', 'class': 'PersonalDataCorporatesStreamingPipeline'},
        {'name':'cards', 'module': 'cards', 'class': 'CardsStreamingPipeline'},
        {'name':'balance_with_other_banks','module':'balance_with_other_banks','class':'BalanceWithOtherBanksStreamingPipeline'},
        {'name':'cash','module':'cash','class':'CashInformationStreamingPipeline'},
        {'name':'balance_with_bot', 'module':'balance_with_bot','class':'BalanceWithBotStreamingPipeline'},
        {'name':'balance_with_mnos','module':'balance_with_mnos','class':'BalanceWithMnosStreamingPipeline'},
        {'name':'loans','module':'loans','class':'LoansStreamingPipeline'},
        {'name':'overdraft','module':'overdraft','class':'OverdraftStreamingPipeline'},
        {'name':'loan_transactions','module':'loan_transactions', 'class':'LoanTransactionsStreamingPipeline'},
        {'name':'deposits','module':'deposits','class':'DepositsStreamingPipeline'},
        {'name':'digital_saving','module':'digital_saving','class':'DigitalSavingStreamingPipeline'},
        {'name':'inter_bank_loan_payable','module':'inter_bank_loan_payable','class':'InterBankLoanPayableStreamingPipeline'},
        {'name':'share_capital','module':'share_capital','class':'ShareCapitalStreamingPipeline'},
        {'name':'income_statement','module':'income_statement','class':'IncomeStatementStreamingPipeline'},
        {'name':'agent_transactions','module':'agent_transactions','class':'AgentTransactionsStreamingPipeline'},
        {'name':'pos_transactions','module':'pos_transactions','class':'POSTransactionsStreamingPipeline'},
        {'name':'atm_transactions','module':'atm_transactions','class':'AtmTransactionsStreamingPipeline'},
        {'name':'card_transactions','module':'card_transactions','class':'CardTransactionsStreamingPipeline'},
        {'name':'mobile_banking','module':'mobile_banking','class':'MobileBankingStreamingPipeline'},
        {'name':'ibcm_transactions','module':'ibcm_transactions','class':'IBCMTransactionsStreamingPipeline'},
        {'name':'incoming_fund_transfer', 'module': 'incoming_fund_transfer', 'class': 'IncomingFundTransferStreamingPipeline'},
        {'name':'outgoing_fund_transfer', 'module': 'outgoing_fund_transfer', 'class': 'OutgoingFundTransferStreamingPipeline'},
        # Add more pipelines here as they are created
    ]
    
    def __init__(self, max_workers=1):  # Sequential = 1 worker
        self.config = Config()
        self.state_manager = PipelineStateManager()
        self.max_workers = max_workers
        self.start_time = datetime.now()
    
    def _import_pipeline(self, pipeline_config):
        """Dynamically import a pipeline module and class"""
        try:
            module_path = f"{pipeline_config['module']}.{pipeline_config['module']}_streaming_pipeline"
            module = __import__(module_path, fromlist=[pipeline_config['class']])
            pipeline_class = getattr(module, pipeline_config['class'])
            return pipeline_class
        except Exception as e:
            logger.error(f"Failed to import {pipeline_config['name']}: {e}")
            return None
    
    def _run_pipeline(self, pipeline_config):
        """Run a single pipeline and return results"""
        pipeline_name = pipeline_config['name']
        start_time = time.time()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting: {pipeline_name}")
        logger.info(f"{'='*60}")
        
        try:
            # Import and instantiate pipeline
            pipeline_class = self._import_pipeline(pipeline_config)
            if pipeline_class is None:
                raise ImportError(f"Cannot import {pipeline_name}")
            
            pipeline = pipeline_class()
            
            # Run the pipeline
            pipeline.run_streaming_pipeline()
            
            # Get records processed from state
            state = self.state_manager.get_all_states()
            records = 0
            for s in state:
                if s[0] == pipeline_name:
                    records = s[4] or 0
                    break
            
            elapsed = time.time() - start_time
            
            # Update state
            self.state_manager.update_run(pipeline_name, 'completed', records)
            
            logger.info(f"✓ {pipeline_name} completed: {records:,} records in {elapsed:.1f}s")
            
            return {
                'name': pipeline_name,
                'status': 'completed',
                'records': records,
                'elapsed': elapsed,
                'error': None
            }
            
        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            
            # Update state with failure
            self.state_manager.update_run(pipeline_name, 'failed', 0, error_msg)
            
            logger.error(f"✗ {pipeline_name} failed: {error_msg}")
            
            return {
                'name': pipeline_name,
                'status': 'failed',
                'records': 0,
                'elapsed': elapsed,
                'error': error_msg
            }
    
    def run_all(self, retry_failed=True):
        """Run all pipelines sequentially"""
        total_start = time.time()
        
        logger.info("\n" + "="*80)
        logger.info("PIPELINE ORCHESTRATOR STARTING")
        logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Pipelines: {len(self.PIPELINES)}")
        logger.info(f"Mode: Sequential (max_workers={self.max_workers})")
        logger.info("="*80)
        
        results = []
        failed_pipelines = []
        
        for pipeline_config in self.PIPELINES:
            result = self._run_pipeline(pipeline_config)
            results.append(result)
            
            if result['status'] == 'failed':
                failed_pipelines.append(result)
        
        # Retry failed pipelines if enabled
        if retry_failed and failed_pipelines:
            logger.info(f"\n{'='*60}")
            logger.info(f"Retrying {len(failed_pipelines)} failed pipelines...")
            logger.info(f"{'='*60}")
            
            for result in failed_pipelines:
                pipeline_name = result['name']
                logger.info(f"\nRetry #{1}: {pipeline_name}")
                
                # Find pipeline config
                pipeline_config = next(
                    (p for p in self.PIPELINES if p['name'] == pipeline_name), 
                    None
                )
                
                if pipeline_config:
                    retry_result = self._run_pipeline(pipeline_config)
                    results.append(retry_result)
                    
                    if retry_result['status'] == 'completed':
                        logger.info(f"✓ Retry successful: {pipeline_name}")
                    else:
                        logger.error(f"✗ Retry failed: {pipeline_name} - {retry_result['error']}")
        
        # Summary
        total_elapsed = time.time() - total_start
        
        completed = [r for r in results if r['status'] == 'completed']
        failed = [r for r in results if r['status'] == 'failed']
        
        total_records = sum(r['records'] for r in completed)
        
        logger.info(f"\n{'='*80}")
        logger.info("ORCHESTRATOR SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Total Time: {total_elapsed/60:.2f} minutes")
        logger.info(f"Completed: {len(completed)}/{len(self.PIPELINES)}")
        logger.info(f"Failed: {len(failed)}/{len(self.PIPELINES)}")
        logger.info(f"Total Records: {total_records:,}")
        logger.info(f"Avg Rate: {total_records/total_elapsed:.1f} records/sec")
        
        if failed:
            logger.info(f"\nFailed Pipelines:")
            for f in failed:
                logger.info(f"  - {f['name']}: {f['error'][:100]}")
        
        logger.info(f"{'='*80}\n")
        
        return results
    
    def get_status(self):
        """Get current status of all pipelines"""
        return self.state_manager.get_all_states()


def main():
    """Main entry point"""
    orchestrator = PipelineOrchestrator(max_workers=1)  # Sequential
    
    try:
        orchestrator.run_all(retry_failed=True)
    except KeyboardInterrupt:
        logger.info("\nOrchestrator interrupted by user")
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()