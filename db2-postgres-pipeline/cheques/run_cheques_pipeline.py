#!/usr/bin/env python3
"""
Simple runner for Cheques Pipeline - no arguments, runs incremental by default
"""

from cheques_streaming_pipeline import ChequesStreamingPipeline

def main():
    """Run cheques pipeline with default settings"""
    pipeline = ChequesStreamingPipeline()
    
    try:
        pipeline.run_streaming_pipeline(incremental=True)
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()