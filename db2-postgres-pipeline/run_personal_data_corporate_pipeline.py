#!/usr/bin/env python3
"""
Runner script for Personal Data Corporate Streaming Pipeline
"""

import sys
import logging
from personal_data_corporate_streaming_pipeline import PersonalDataCorporateStreamingPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    logger.info("="*80)
    logger.info("STARTING PERSONAL DATA CORPORATE STREAMING PIPELINE")
    logger.info("="*80)
    
    try:
        pipeline = PersonalDataCorporateStreamingPipeline(batch_size=100)
        pipeline.run_streaming_pipeline()
        
        logger.info("="*80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
