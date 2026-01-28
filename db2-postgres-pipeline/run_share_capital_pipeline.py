#!/usr/bin/env python3
"""
Share Capital Pipeline Runner
"""

from share_capital_streaming_pipeline import ShareCapitalStreamingPipeline
import logging

def main():
    """Run the share capital streaming pipeline"""
    print("Share Capital Pipeline Runner")
    print("=" * 65)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Create and run pipeline with optimized settings
        pipeline = ShareCapitalStreamingPipeline(batch_size=50)
        
        logger.info("Starting Share Capital pipeline...")
        logger.info("Using optimized cursor-based pagination for better performance...")
        
        # Run the streaming pipeline
        pipeline.run_streaming_pipeline()
        
        logger.info("Share Capital pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()