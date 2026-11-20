#!/usr/bin/env python3
"""
Run Fraud Incidence Streaming Pipeline
"""

import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fraud_incidence_streaming_pipeline import FraudIncidenceStreamingPipeline

def main():
    """Main function to run the fraud incidence streaming pipeline"""

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('../logs/fraud_incidence_pipeline.log'),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting Fraud Incidence Streaming Pipeline...")

        pipeline = FraudIncidenceStreamingPipeline(
            batch_size=1000,
            consumer_batch_size=100
        )

        pipeline.run_streaming_pipeline()

        logger.info("Fraud Incidence Pipeline completed successfully!")

    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
