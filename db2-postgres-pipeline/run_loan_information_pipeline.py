#!/usr/bin/env python3
"""
Run the loan information streaming pipeline
"""

from loan_information_streaming_pipeline import LoanInformationStreamingPipeline

def main():
    # Create pipeline with optimized batch size
    pipeline = LoanInformationStreamingPipeline(batch_size=1000)  # Increased from 50 to 1000
    
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