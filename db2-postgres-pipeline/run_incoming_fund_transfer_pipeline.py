#!/usr/bin/env python3
"""
Incoming Fund Transfer Pipeline Runner
"""

from incoming_fund_transfer_streaming_pipeline import IncomingFundTransferStreamingPipeline
import logging

def main():
    """Run the incoming fund transfer streaming pipeline"""
    print("Incoming Fund Transfer Pipeline Runner")
    print("=" * 65)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Create and run pipeline with optimized settings
        pipeline = IncomingFundTransferStreamingPipeline(batch_size=50)
        
        logger.info("Starting Incoming Fund Transfer pipeline...")
        logger.info("Using optimized cursor-based pagination for better performance...")
        
        # Run the streaming pipeline
        pipeline.run_streaming_pipeline()
        
        logger.info("Incoming Fund Transfer pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()