#!/usr/bin/env python3
"""
ICBM Transaction Pipeline Runner
"""

from icbm_transaction_streaming_pipeline import IcbmTransactionStreamingPipeline
import logging

def main():
    """Run the ICBM transaction streaming pipeline"""
    print("ICBM Transaction Pipeline Runner")
    print("=" * 60)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Create and run pipeline with optimized settings
        pipeline = IcbmTransactionStreamingPipeline(batch_size=50)
        
        logger.info("Starting ICBM Transaction pipeline...")
        logger.info("Using optimized cursor-based pagination for better performance...")
        
        # Run the streaming pipeline
        pipeline.run_streaming_pipeline()
        
        logger.info("ICBM Transaction pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()