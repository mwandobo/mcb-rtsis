#!/usr/bin/env python3
"""
Run Cash Information Streaming Pipeline
"""

import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cash_streaming_pipeline import CashInformationStreamingPipeline

def main():
    """Main function to run the cash information streaming pipeline"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('../logs/cash_information_pipeline.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Cash Information Streaming Pipeline...")
        
        # Create and run pipeline with default settings
        pipeline = CashInformationStreamingPipeline(
            batch_size=1000,
            consumer_batch_size=100
        )
        
        pipeline.run_streaming_pipeline()
        
        logger.info("Cash Information Pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()