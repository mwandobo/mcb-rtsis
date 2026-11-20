#!/usr/bin/env python3
"""
Run Written Off Loans Streaming Pipeline
"""

import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from written_off_loans_streaming_pipeline import WrittenOffLoansStreamingPipeline

def main():
    """Main function to run the written off loans streaming pipeline"""

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('../logs/written_off_loans_pipeline.log'),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting Written Off Loans Streaming Pipeline...")

        # Create and run pipeline with default settings
        pipeline = WrittenOffLoansStreamingPipeline(
            batch_size=1000,
            consumer_batch_size=100
        )

        pipeline.run_streaming_pipeline()

        logger.info("Written Off Loans Pipeline completed successfully!")

    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
