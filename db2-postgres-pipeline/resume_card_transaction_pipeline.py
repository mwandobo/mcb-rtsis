#!/usr/bin/env python3
"""
Resume Card Transaction Pipeline - BOT Project
Continues processing from where it left off
"""

from card_transaction_pipeline import CardTransactionPipeline
import logging

def main():
    """Resume card transaction pipeline"""
    
    print("🔄 Resume Card Transaction Pipeline - BOT Project")
    print("=" * 60)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        # Create pipeline with resume enabled
        pipeline = CardTransactionPipeline(
            start_date='2024-01-01', 
            resume=True,
            limit=1000  # Process in batches of 1000
        )
        
        logger.info("🔄 Starting resume from last processed record...")
        pipeline.run_complete_pipeline()
        
    except KeyboardInterrupt:
        logger.info("⏹️ Pipeline stopped by user. You can resume again by running this script.")
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()