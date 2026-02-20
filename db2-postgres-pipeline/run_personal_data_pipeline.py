#!/usr/bin/env python3
"""
Run the personal data streaming pipeline with v3 query (CTEs removed for performance)
"""

from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

def main():
    # Create pipeline with batch size of 500 (like agents/POS)
    pipeline = PersonalDataStreamingPipeline(batch_size=500)
    
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
