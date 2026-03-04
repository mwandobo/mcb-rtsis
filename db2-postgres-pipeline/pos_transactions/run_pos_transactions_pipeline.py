#!/usr/bin/env python3
"""
Run POS Transactions Streaming Pipeline
"""
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from pos_transactions_streaming_pipeline import POSTransactionsStreamingPipeline

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("STARTING POS TRANSACTIONS STREAMING PIPELINE")
    logger.info("=" * 60)
    try:
        pipeline = POSTransactionsStreamingPipeline(batch_size=1000, consumer_batch_size=100)
        pipeline.run_streaming_pipeline()
        logger.info("=" * 60)
        logger.info("POS TRANSACTIONS PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        logger.error("=" * 60)
        logger.error("POS TRANSACTIONS PIPELINE FAILED")

if __name__ == "__main__":
    main()
