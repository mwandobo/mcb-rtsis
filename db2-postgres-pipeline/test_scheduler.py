#!/usr/bin/env python3
"""
Test Scheduler - Runs a single pipeline every 5 minutes to verify the system works
"""

import sys
import os
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from pipeline_state import PipelineStateManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TestScheduler:
    """Test scheduler with a single pipeline"""
    
    def __init__(self, interval_minutes=5):
        self.config = Config()
        self.interval_minutes = interval_minutes
        self.scheduler = BackgroundScheduler()
        self.state_manager = PipelineStateManager()
        
        # Test pipeline configuration
        self.test_pipeline = {
            'name': 'personal_data_corporates',
            'module': 'personal_data_corporates',
            'class': 'PersonalDataCorporatesStreamingPipeline'
        }
        
        logging.getLogger('apscheduler').setLevel(logging.WARNING)
    
    def _import_pipeline(self):
        """Import the test pipeline"""
        try:
            module_path = f"{self.test_pipeline['module']}.{self.test_pipeline['module']}_streaming_pipeline"
            module = __import__(module_path, fromlist=[self.test_pipeline['class']])
            pipeline_class = getattr(module, self.test_pipeline['class'])
            return pipeline_class
        except Exception as e:
            logger.error(f"Failed to import pipeline: {e}")
            return None
    
    def run_pipeline_job(self):
        """Run the test pipeline"""
        pipeline_name = self.test_pipeline['name']
        start_time = datetime.now()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"SCHEDULED RUN: {pipeline_name}")
        logger.info(f"Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        
        try:
            # Import and run pipeline
            pipeline_class = self._import_pipeline()
            if pipeline_class is None:
                raise ImportError(f"Cannot import {pipeline_name}")
            
            pipeline = pipeline_class()
            pipeline.run_streaming_pipeline()
            
            # Get records from state
            states = self.state_manager.get_all_states()
            records = 0
            for s in states:
                if s[0] == pipeline_name:
                    records = s[4] or 0
                    break
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            # Update state
            self.state_manager.update_run(pipeline_name, 'completed', records)
            
            logger.info(f"✓ {pipeline_name} completed: {records:,} records in {elapsed:.1f}s")
            
        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            error_msg = str(e)
            
            self.state_manager.update_run(pipeline_name, 'failed', 0, error_msg)
            
            logger.error(f"✗ {pipeline_name} failed: {error_msg}")
    
    def start(self):
        """Start the test scheduler"""
        logger.info("\n" + "="*60)
        logger.info("TEST SCHEDULER")
        logger.info(f"Pipeline: {self.test_pipeline['name']}")
        logger.info(f"Interval: every {self.interval_minutes} minutes")
        logger.info("="*60)
        
        # Add the job
        self.scheduler.add_job(
            func=self.run_pipeline_job,
            trigger=IntervalTrigger(minutes=self.interval_minutes),
            id='test_pipeline',
            name='Test Pipeline',
            replace_existing=True,
            max_instances=1
        )
        
        self.scheduler.start()
        
        logger.info(f"Scheduler started. Next run in {self.interval_minutes} minutes.")
        logger.info("Press Ctrl+C to stop.\n")
        
        return self.scheduler
    
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped")


def main():
    scheduler = TestScheduler(interval_minutes=1)
    
    try:
        scheduler.start()
        
        import time
        while True:
            time.sleep(1)
            
    except (KeyboardInterrupt, SystemExit):
        logger.info("\nShutting down scheduler...")
        scheduler.stop()


if __name__ == "__main__":
    main()