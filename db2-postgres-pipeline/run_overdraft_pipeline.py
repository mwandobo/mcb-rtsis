#!/usr/bin/env python3
"""
Overdraft Pipeline Runner
"""

from overdraft_streaming_pipeline import OverdraftStreamingPipeline
import logging

def main():
    """Run the overdraft streaming pipeline"""
    print("Overdraft Pipeline Runner")
    print("=" * 55)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Create and run pipeline with optimized settings
        pipeline = OverdraftStreamingPipeline(batch_size=50)
        
        logger.info("Starting Overdraft pipeline...")
        logger.info("Using optimized cursor-based pagination for better performance...")
        
        # Run the streaming pipeline
        pipeline.run_streaming_pipeline()
        
        logger.info("Overdraft pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()