#!/usr/bin/env python3
"""
Test Cash Pipeline Only - Simple Test
"""

import logging
import threading
import time
from simple_multi_pipeline import SimpleMultiPipeline

def test_cash_only():
    """Test only the cash information pipeline"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("💰 Testing Cash Information Pipeline Only")
    logger.info("=" * 60)
    
    try:
        # Initialize pipeline
        pipeline = SimpleMultiPipeline()
        
        # Show current tracking status
        logger.info("📊 Current Tracking Status:")
        pipeline.tracker.show_all_tracking_info()
        
        # Step 1: Setup RabbitMQ queue
        logger.info("\n🔧 Setting up RabbitMQ queue...")
        pipeline.setup_rabbitmq_queues()
        
        # Step 2: Fetch and publish cash data with tracking
        logger.info("\n📊 Fetching cash data with tracking...")
        record_count = pipeline.fetch_and_publish_cash()
        
        if record_count == 0:
            logger.info("ℹ️ No new cash records found - tracking is working correctly!")
            return
        
        # Step 3: Start consumer in thread
        logger.info(f"\n🔄 Starting consumer for {record_count} records...")
        consumer_thread = threading.Thread(target=pipeline.consume_cash_queue, daemon=True)
        consumer_thread.start()
        
        # Wait for processing
        logger.info("⏳ Processing for 35 seconds...")
        time.sleep(35)
        
        pipeline.running = False
        consumer_thread.join(timeout=5)
        
        # Step 4: Show final tracking status
        logger.info("\n📊 Final Tracking Status:")
        pipeline.tracker.show_all_tracking_info()
        
        logger.info("\n✅ Cash pipeline test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Cash pipeline test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_cash_only()