#!/usr/bin/env python3
"""
Run Employee Streaming Pipeline
"""

import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from employee_streaming_pipeline import EmployeeStreamingPipeline

def main():
    """Main function to run the employee streaming pipeline"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('../logs/employee_pipeline.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🏢 EMPLOYEE STREAMING PIPELINE")
        logger.info("=" * 60)
        logger.info("📋 Features:")
        logger.info("  - Uses employee-v1.sql query")
        logger.info("  - Producer and Consumer run SIMULTANEOUSLY")
        logger.info("  - Real-time processing as data arrives")
        logger.info("  - Salary CTE with random salary generation")
        logger.info("  - Employee position and department mapping")
        logger.info("  - NHIF and PSSSF calculations")
        logger.info("  - Batch size: 1000 records per batch")
        logger.info("=" * 60)
        
        # Create and run pipeline with default settings
        pipeline = EmployeeStreamingPipeline(
            batch_size=1000,
            consumer_batch_size=100
        )
        
        pipeline.run_streaming_pipeline()
        
        logger.info("=" * 60)
        logger.info("✅ EMPLOYEE STREAMING PIPELINE COMPLETED!")
        logger.info("🔍 Key features used:")
        logger.info("  - Full employee-v1.sql implementation")
        logger.info("  - Real-time processing (no queue buildup)")
        logger.info("  - Producer and consumer worked simultaneously")
        logger.info("  - Salary CTE with random generation")
        logger.info("  - Employee position categorization")
        logger.info("  - NHIF/PSSSF benefit calculations")
        logger.info("  - Memory efficient")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()