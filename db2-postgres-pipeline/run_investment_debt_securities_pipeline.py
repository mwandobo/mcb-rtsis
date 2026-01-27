#!/usr/bin/env python3
"""
Run Investment Debt Securities Pipeline
Simple runner script for the investment debt securities streaming pipeline
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from investment_debt_securities_pipeline import InvestmentDebtSecuritiesStreamingPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the investment debt securities pipeline"""
    
    print("Investment Debt Securities Pipeline Runner")
    print("=" * 55)
    
    try:
        # Create pipeline with batch size of 10
        pipeline = InvestmentDebtSecuritiesStreamingPipeline(batch_size=10)
        
        # Run the streaming pipeline to get all records
        logger.info("Starting Investment Debt Securities pipeline...")
        pipeline.run_streaming_pipeline()  # Process ALL records
        
        print("\nInvestment Debt Securities pipeline completed successfully!")
        
    except KeyboardInterrupt:
        print("\nPipeline stopped by user")
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        logger.error(f"Pipeline error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()