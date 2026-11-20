#!/usr/bin/env python3
"""
Simple runner for IBCM Transactions Pipeline
Runs incremental processing by default (no arguments needed)
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ibcm_transactions.ibcm_transactions_streaming_pipeline import IBCMTransactionsStreamingPipeline

def main():
    """Run IBCM transactions pipeline"""
    pipeline = IBCMTransactionsStreamingPipeline()

    full_load = "--full-load" in sys.argv
    incremental = not full_load

    if full_load:
        pipeline.logger.info("Running in FULL LOAD mode (--full-load flag detected)")

    try:
        pipeline.run_streaming_pipeline(incremental=incremental)
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()