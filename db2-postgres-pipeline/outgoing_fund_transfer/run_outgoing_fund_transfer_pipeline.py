#!/usr/bin/env python3
"""
Run Outgoing Fund Transfer Streaming Pipeline
"""

import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from outgoing_fund_transfer_streaming_pipeline import OutgoingFundTransferStreamingPipeline

def main():
    """Main function to run the outgoing fund transfer streaming pipeline"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('../logs/outgoing_fund_transfer_pipeline.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Outgoing Fund Transfer Streaming Pipeline...")
        
        # Create and run pipeline with default settings
        pipeline = OutgoingFundTransferStreamingPipeline(
            batch_size=1000,
            consumer_batch_size=100
        )
        
        pipeline.run_streaming_pipeline()
        
        logger.info("Outgoing Fund Transfer Pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()