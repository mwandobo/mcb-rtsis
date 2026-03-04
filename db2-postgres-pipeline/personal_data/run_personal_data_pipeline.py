#!/usr/bin/env python3
"""
Run Personal Data Streaming Pipeline
"""
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Use optimized v4 by default, allow override via environment variable
    sql_version = os.getenv('SQL_VERSION', 'v4')
    
    logger.info("=" * 60)
    logger.info(f"STARTING PERSONAL DATA STREAMING PIPELINE (SQL {sql_version.upper()})")
    logger.info("=" * 60)
    try:
        pipeline = PersonalDataStreamingPipeline(batch_size=1000, consumer_batch_size=100, sql_version=sql_version)
        pipeline.run_streaming_pipeline()
        logger.info("=" * 60)
        logger.info("PERSONAL DATA PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        logger.error("=" * 60)
        logger.error("PERSONAL DATA PIPELINE FAILED")

if __name__ == "__main__":
    main()