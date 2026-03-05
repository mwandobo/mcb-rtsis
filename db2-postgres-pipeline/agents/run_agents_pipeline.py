#!/usr/bin/env python3
"""
Run Agents Streaming Pipeline
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents_streaming_pipeline import AgentsStreamingPipeline

def main():
    """Main function to run the agents streaming pipeline"""
    
    pipeline = AgentsStreamingPipeline(batch_size=1000, consumer_batch_size=100)
    
    try:
        pipeline.run_streaming_pipeline()
        
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
