#!/usr/bin/env python3
"""
Simple runner for Interbank Loans Payable Pipeline
Runs incremental processing by default (no arguments needed)
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interbank_loans_payable.interbank_loans_payable_streaming_pipeline import InterbankLoansPayableStreamingPipeline

def main():
    """Run interbank loans payable pipeline in incremental mode"""
    pipeline = InterbankLoansPayableStreamingPipeline()
    
    try:
        pipeline.run_streaming_pipeline(incremental=True)
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()