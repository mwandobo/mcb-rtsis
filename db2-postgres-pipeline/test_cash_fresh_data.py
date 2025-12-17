#!/usr/bin/env python3
"""
Test Cash Pipeline with Fresh Data - Reset Tracking
"""

import logging
import threading
import time
from simple_multi_pipeline import SimpleMultiPipeline

def test_cash_with_fresh_data():
    """Test cash pipeline with fresh data by resetting tracking"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("💰 Testing Cash Pipeline with Fresh Data")
    logger.info("=" * 60)
    
    try:
        # Initialize pipeline
        pipeline = SimpleMultiPipeline()
        
        # Show current tracking status
        logger.info("📊 Current Tracking Status:")
        pipeline.tracker.show_all_tracking_info()
        
        # Reset tracking to get fresh data (go back 7 days)
        logger.info("\n🔄 Resetting tracking to get fresh data...")
        pipeline.tracker.reset_tracking('cash_information')
        
        # Show tracking after reset
        logger.info("\n📊 Tracking Status After Reset:")
        pipeline.tracker.show_all_tracking_info()
        
        # Step 1: Setup RabbitMQ queue
        logger.info("\n🔧 Setting up RabbitMQ queue...")
        pipeline.setup_rabbitmq_queues()
        
        # Step 2: Fetch and publish cash data with tracking
        logger.info("\n📊 Fetching cash data with fresh tracking...")
        record_count = pipeline.fetch_and_publish_cash()
        
        if record_count == 0:
            logger.info("ℹ️ No cash records found in the last 7 days")
            logger.info("💡 This might be normal if there's no recent transaction data")
            return
        
        logger.info(f"✅ Found {record_count} cash records!")
        
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
        
        # Check PostgreSQL results
        logger.info("\n📋 Checking PostgreSQL results...")
        try:
            with pipeline.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM cash_information")
                count = cursor.fetchone()[0]
                logger.info(f"💾 Total records in PostgreSQL: {count:,}")
                
                # Show sample records
                cursor.execute("SELECT * FROM cash_information ORDER BY \"transactionDate\" DESC LIMIT 3")
                records = cursor.fetchall()
                
                logger.info("📋 Sample records:")
                for i, record in enumerate(records, 1):
                    logger.info(f"  {i}. Date: {record[7]} | Branch: {record[1]} | Category: {record[2]} | Amount: {record[4]:,.2f} {record[3]}")
                    
        except Exception as e:
            logger.error(f"❌ PostgreSQL check error: {e}")
        
    except Exception as e:
        logger.error(f"❌ Cash pipeline test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_cash_with_fresh_data()