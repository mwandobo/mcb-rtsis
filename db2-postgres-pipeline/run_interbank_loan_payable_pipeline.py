#!/usr/bin/env python3
"""
Run Interbank Loan Payable Pipeline
Simple runner script for the interbank loan payable streaming pipeline
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from interbank_loan_payable_streaming_pipeline import InterbankLoanPayableStreamingPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the interbank loan payable pipeline"""
    
    print("Interbank Loan Payable Pipeline Runner")
    print("=" * 55)
    
    try:
        # Create pipeline with batch size of 5 (optimized for large records)
        pipeline = InterbankLoanPayableStreamingPipeline(batch_size=5)
        
        # Run the streaming pipeline to get all records
        logger.info("Starting Interbank Loan Payable pipeline...")
        logger.info("Processing 12.2+ million records with enhanced retry logic...")
        pipeline.run_streaming_pipeline()  # Process ALL records
        
        print("\nInterbank Loan Payable pipeline completed successfully!")
        
    except KeyboardInterrupt:
        print("\nPipeline stopped by user")
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        logger.error(f"Pipeline error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()