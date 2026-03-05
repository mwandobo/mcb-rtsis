#!/usr/bin/env python3
"""
Run ATM Streaming Pipeline
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from atm_streaming_pipeline import AtmStreamingPipeline

def main():
    """Main function to run the ATM streaming pipeline"""
    
    # Create pipeline with default settings
    pipeline = AtmStreamingPipeline(batch_size=1000, consumer_batch_size=100)
    
    try:
        # Run the streaming pipeline
        pipeline.run_streaming_pipeline()
        
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
