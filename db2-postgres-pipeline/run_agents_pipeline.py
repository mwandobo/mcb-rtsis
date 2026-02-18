#!/usr/bin/env python3
"""
Run the agents streaming pipeline
"""

from agents_streaming_pipeline import AgentsStreamingPipeline

def main():
    # Create pipeline with optimized batch size
    pipeline = AgentsStreamingPipeline(batch_size=500)
    
    try:
        # Run the streaming pipeline (both producer and consumer)
        pipeline.run_streaming_pipeline()
        
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline error: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
