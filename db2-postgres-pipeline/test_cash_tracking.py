#!/usr/bin/env python3
"""
Test Cash Pipeline with Professional Tracking
"""

import logging
from pipeline_tracker import PipelineTracker
from simple_multi_pipeline import SimpleMultiPipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_cash_tracking():
    """Test cash pipeline with tracking"""
    
    logger.info("🚀 Testing Cash Pipeline with Professional Tracking")
    logger.info("=" * 60)
    
    # Initialize tracker and pipeline
    tracker = PipelineTracker()
    pipeline = SimpleMultiPipeline()
    
    # Show current tracking status
    logger.info("📊 Current Tracking Status:")
    tracker.show_all_tracking_info()
    
    # Test cash pipeline
    try:
        # Setup queue
        pipeline.setup_rabbitmq_queues()
        
        # Fetch and publish with tracking
        logger.info("\n💰 Testing Cash Data Fetch with Tracking...")
        record_count = pipeline.fetch_and_publish_cash()
        
        if record_count > 0:
            logger.info(f"✅ Successfully processed {record_count} cash records")
        else:
            logger.info("ℹ️ No new records to process (tracking working correctly)")
        
        # Show updated tracking status
        logger.info("\n📊 Updated Tracking Status:")
        tracker.show_all_tracking_info()
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

def reset_cash_tracking():
    """Reset cash tracking for testing"""
    logger.info("🔄 Resetting Cash Tracking for Testing")
    
    tracker = PipelineTracker()
    tracker.reset_tracking('cash_information')
    
    logger.info("✅ Cash tracking reset completed")

def set_manual_start_point():
    """Set a manual starting point for testing"""
    
    # ============================================
    # 🔧 CONFIGURE STARTING TIMESTAMP HERE
    # ============================================
    MANUAL_START_TIMESTAMP = '2024-12-01 00:00:00'  # Change this as needed
    # ============================================
    
    logger.info(f"🔧 Setting manual start timestamp: {MANUAL_START_TIMESTAMP}")
    
    tracker = PipelineTracker()
    tracker.set_last_processed_timestamp('cash_information', MANUAL_START_TIMESTAMP)
    
    logger.info("✅ Manual start timestamp set")
    tracker.show_all_tracking_info()

def main():
    """Main test function"""
    
    print("\n💰 Cash Pipeline Tracking Test Options:")
    print("1. Test current tracking status")
    print("2. Reset tracking (start fresh)")
    print("3. Set manual start point")
    print("4. Run full test")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == '1':
        tracker = PipelineTracker()
        tracker.show_all_tracking_info()
    elif choice == '2':
        reset_cash_tracking()
    elif choice == '3':
        set_manual_start_point()
    elif choice == '4':
        test_cash_tracking()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()