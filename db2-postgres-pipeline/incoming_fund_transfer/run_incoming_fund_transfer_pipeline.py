#!/usr/bin/env python3
"""
Simple runner for Incoming Fund Transfer Pipeline
Runs incremental processing by default (no arguments needed)
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from incoming_fund_transfer.incoming_fund_transfer_streaming_pipeline import IncomingFundTransferStreamingPipeline

def main():
    """Run incoming fund transfer pipeline in incremental mode"""
    pipeline = IncomingFundTransferStreamingPipeline()
    
    try:
        pipeline.run_streaming_pipeline(incremental=True)
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()