#!/usr/bin/env python3
"""
Run the deposits streaming pipeline
"""

import logging
import sys
import time

from deposits_streaming_pipeline import DepositsStreamingPipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("DEPOSITS PIPELINE RUNNER")
    logger.info("=" * 60)
    logger.info("Note: Run create_deposits_table.py first if table doesn't exist")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    try:
        logger.info("Running pipeline")
        logger.info("=" * 60)
        logger.info("")
        
        pipeline = DepositsStreamingPipeline(batch_size=1000, consumer_batch_size=100)
        pipeline.run_streaming_pipeline()
        
        elapsed_time = (time.time() - start_time) / 60
        
        logger.info("")
        logger.info("✓ Running pipeline completed")
        logger.info("=" * 60)
        logger.info("COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Time: {elapsed_time:.2f} min")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.info("\n✗ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
