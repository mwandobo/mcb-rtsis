#!/usr/bin/env python3
"""
Pipeline Scheduler - Runs all pipelines every 5 minutes using APScheduler
"""

import sys
import os
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from run_all_pipelines import PipelineOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Schedules pipeline runs every 5 minutes"""
    
    def __init__(self, interval_minutes=5):
        self.config = Config()
        self.interval_minutes = interval_minutes
        self.scheduler = BackgroundScheduler()
        self.orchestrator = PipelineOrchestrator(max_workers=1)
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging for scheduler events"""
        # Reduce noise from APScheduler
        logging.getLogger('apscheduler').setLevel(logging.WARNING)
    
    def _job_listener(self, event):
        """Listen for job events"""
        if event.exception:
            logger.error(f"Job {event.job_id} failed: {event.exception}")
        else:
            logger.debug(f"Job {event.job_id} executed successfully")
    
    def run_pipeline_job(self):
        """The job that runs all pipelines"""
        start_time = datetime.now()
        logger.info("\n" + "="*80)
        logger.info(f"SCHEDULED RUN STARTED: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        try:
            self.orchestrator.run_all(retry_failed=True)
        except Exception as e:
            logger.error(f"Scheduled run failed: {e}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Scheduled run completed in {elapsed:.1f} seconds")
    
    def start(self):
        """Start the scheduler"""
        logger.info("\n" + "="*80)
        logger.info("PIPELINE SCHEDULER")
        logger.info(f"Interval: every {self.interval_minutes} minutes")
        logger.info("="*80)
        
        # Add the job
        self.scheduler.add_job(
            func=self.run_pipeline_job,
            trigger=IntervalTrigger(minutes=self.interval_minutes),
            id='run_all_pipelines',
            name='Run All Pipelines',
            replace_existing=True,
            max_instances=1  # Only one instance at a time
        )
        
        # Add event listener
        self.scheduler.add_listener(
            self._job_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED
        )
        
        # Start the scheduler
        self.scheduler.start()
        
        logger.info(f"Scheduler started. Next run in {self.interval_minutes} minutes.")
        logger.info("Press Ctrl+C to stop.\n")
        
        return self.scheduler
    
    def stop(self):
        """Stop the scheduler gracefully"""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped")


def main():
    """Main entry point"""
    scheduler = PipelineScheduler(interval_minutes=5)
    
    try:
        scheduler.start()
        
        # Keep the main thread alive
        import time
        while True:
            time.sleep(1)
            
    except (KeyboardInterrupt, SystemExit):
        logger.info("\nShutting down scheduler...")
        scheduler.stop()


if __name__ == "__main__":
    main()