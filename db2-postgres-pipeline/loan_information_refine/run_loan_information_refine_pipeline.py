#!/usr/bin/env python3
"""
Run Loan Information Refine Streaming Pipeline
"""

import sys
import os
import logging
import argparse

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loan_information_refine_streaming_pipeline import LoanInformationRefineStreamingPipeline

def main():
    """Main function to run the loan information refine streaming pipeline"""

    parser = argparse.ArgumentParser(description='Run Loan Information Refine Pipeline')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('../logs/loan_information_refine_pipeline.log'),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting Loan Information Refine Streaming Pipeline...")

        # Create and run pipeline with default settings
        pipeline = LoanInformationRefineStreamingPipeline(batch_size=1000)

        pipeline.run_streaming_pipeline(incremental=not args.full_load)

        logger.info("Loan Information Refine Pipeline completed successfully!")

    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
