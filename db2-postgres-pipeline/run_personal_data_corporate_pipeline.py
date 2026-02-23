#!/usr/bin/env python3
"""
Runner script for Personal Data Corporate V4 Streaming Pipeline
"""

import sys
import logging
from personal_data_corporate_streaming_pipeline import PersonalDataCorporateStreamingPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    print("🏢 RUNNING PERSONAL DATA CORPORATE V4 STREAMING PIPELINE")
    print("=" * 70)
    print("📋 V4 Features:")
    print("  - Uses personal-data-corporates-v4.sql")
    print("  - Location mapping with bank_location_lookup_v2")
    print("  - numberOfEmployees field added")
    print("  - Better region/district/ward mapping")
    print("  - Batch size: 500 records (optimized)")
    print("  - Producer + Consumer simultaneous streaming")
    print("=" * 70)
    
    logger.info("="*80)
    logger.info("STARTING PERSONAL DATA CORPORATE V4 STREAMING PIPELINE")
    logger.info("="*80)
    
    try:
        # Use optimized batch size of 500
        pipeline = PersonalDataCorporateStreamingPipeline(batch_size=500)
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 70)
        print("✅ CORPORATE V4 PIPELINE COMPLETED!")
        print(f"📊 Total corporate records processed: {count or 0:,}")
        print("🔍 V4 Improvements:")
        print("  - Enhanced location mapping")
        print("  - Complete field mappings")
        print("  - Better data accuracy")
        print("  - Optimized performance")
        print("=" * 70)
        
        logger.info("="*80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        print(f"\n❌ Corporate V4 pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
