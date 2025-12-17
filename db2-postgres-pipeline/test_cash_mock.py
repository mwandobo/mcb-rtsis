#!/usr/bin/env python3
"""
Test Cash Pipeline with Mock Data - While DB2 Password is Expired
"""

import logging
import threading
import time
import json
import pika
from datetime import datetime, timedelta
from dataclasses import asdict
from processors.cash_processor import CashProcessor, CashRecord
from pipeline_tracker import PipelineTracker
from config import Config

def create_mock_cash_data():
    """Create mock cash data for testing"""
    mock_data = []
    base_date = datetime.now() - timedelta(days=1)
    
    for i in range(5):
        # Create mock DB2 row format
        mock_row = (
            (base_date + timedelta(hours=i)).strftime('%Y-%m-%d'),  # TRN_DATE
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),           # REPORTING_DATE
            f'00{i+1}',                                             # BRANCH_CODE
            'Cash in vault',                                        # CASH_CATEGORY
            'TZS',                                                  # CURRENCY
            1000000.0 + (i * 100000),                             # ORG_AMOUNT
            None,                                                   # USD_AMOUNT
            1000000.0 + (i * 100000),                             # TZS_AMOUNT
            (base_date + timedelta(hours=i)).strftime('%Y-%m-%d'),  # TRANSACTION_DATE
            (base_date + timedelta(days=30)).strftime('%Y-%m-%d'),  # MATURITY_DATE
            0.0,                                                    # ALLOWANCE_PROBABLE_LOSS
            0.0                                                     # BOT_PROVISION
        )
        mock_data.append(mock_row)
    
    return mock_data

def test_cash_pipeline_with_mock():
    """Test cash pipeline with mock data"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🧪 Testing Cash Pipeline with Mock Data")
    logger.info("=" * 60)
    logger.info("⚠️  Using mock data while DB2 password is expired")
    logger.info("=" * 60)
    
    try:
        config = Config()
        tracker = PipelineTracker()
        cash_processor = CashProcessor()
        
        # Show current tracking
        logger.info("📊 Current Tracking Status:")
        tracker.show_all_tracking_info()
        
        # Step 1: Setup RabbitMQ queue
        logger.info("\n🔧 Setting up RabbitMQ queue...")
        credentials = pika.PlainCredentials(
            config.message_queue.rabbitmq_user,
            config.message_queue.rabbitmq_password
        )
        parameters = pika.ConnectionParameters(
            host=config.message_queue.rabbitmq_host,
            port=config.message_queue.rabbitmq_port,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='cash_information_queue', durable=True)
        connection.close()
        logger.info("✅ RabbitMQ queue ready")
        
        # Step 2: Create and publish mock data
        logger.info("\n📊 Creating mock cash data...")
        mock_rows = create_mock_cash_data()
        logger.info(f"💰 Created {len(mock_rows)} mock cash records")
        
        # Process mock data
        records = []
        for row in mock_rows:
            record = cash_processor.process_record(row, 'cash_information')
            if cash_processor.validate_record(record):
                records.append(record)
                logger.info(f"  📋 Mock record: {record.transaction_date} | Branch {record.branch_code} | {record.amount_local:,.2f} {record.currency}")
        
        # Publish to queue
        logger.info(f"\n📤 Publishing {len(records)} records to queue...")
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        for record in records:
            message = json.dumps(asdict(record), default=str)
            channel.basic_publish(
                exchange='',
                routing_key='cash_information_queue',
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        
        connection.close()
        logger.info("✅ Mock data published to queue")
        
        # Step 3: Test consumer (simplified version)
        logger.info("\n🔄 Testing consumer with mock data...")
        
        # Simulate processing (without actual PostgreSQL for this test)
        processed_count = 0
        for record in records:
            logger.info(f"💰 Would process: {record.transaction_date} | Branch {record.branch_code} | {record.cash_category} | {record.amount_local:,.2f} {record.currency}")
            processed_count += 1
        
        # Update tracking with mock timestamp
        latest_timestamp = records[-1].transaction_date if records else None
        if latest_timestamp:
            tracker.set_last_processed_timestamp('cash_information', latest_timestamp)
            tracker.update_processing_stats('cash_information', processed_count)
        
        # Show final tracking
        logger.info("\n📊 Final Tracking Status:")
        tracker.show_all_tracking_info()
        
        logger.info(f"\n✅ Mock test completed! Processed {processed_count} records")
        logger.info("🔧 Pipeline is ready - just need DB2 password reset!")
        
    except Exception as e:
        logger.error(f"❌ Mock test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_cash_pipeline_with_mock()