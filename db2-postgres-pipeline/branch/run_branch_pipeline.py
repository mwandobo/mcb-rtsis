#!/usr/bin/env python3
"""
Run Branch Streaming Pipeline
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from branch_streaming_pipeline import BranchStreamingPipeline

def main():
    """Main function to run the branch streaming pipeline"""
    
    pipeline = BranchStreamingPipeline(batch_size=1000, consumer_batch_size=100)
    
    try:
        pipeline.run_streaming_pipeline()
        
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
