#!/usr/bin/env python3
"""
Run Personal Data Corporate Pipeline
Simple runner script for the personal data corporate streaming pipeline
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from personal_data_corporate_streaming_pipeline import PersonalDataCorporateStreamingPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the personal data corporate pipeline"""
    
    print("Personal Data Corporate Pipeline Runner")
    print("=" * 50)
    
    try:
        # Create pipeline with batch size of 10
        pipeline = PersonalDataCorporateStreamingPipeline(batch_size=10)
        
        # Run the streaming pipeline with no limit to get all records
        logger.info("Starting Personal Data Corporate pipeline...")
        pipeline.run_streaming_pipeline(limit=None)  # Process ALL records
        
        print("\nPersonal Data Corporate pipeline completed successfully!")
        
    except KeyboardInterrupt:
        print("\nPipeline stopped by user")
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        logger.error(f"Pipeline error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()