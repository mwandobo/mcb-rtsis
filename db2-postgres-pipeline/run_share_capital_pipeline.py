#!/usr/bin/env python3
"""
Runner script for Share Capital Streaming Pipeline
"""

import sys
import logging
from share_capital_streaming_pipeline import ShareCapitalStreamingPipeline

def main():
    """Main function to run the pipeline"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("Starting Share Capital Streaming Pipeline")
    logger.info("=" * 60)
    
    try:
        pipeline = ShareCapitalStreamingPipeline(batch_size=500)
        pipeline.run_streaming_pipeline()
        
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.info("\nPipeline stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()