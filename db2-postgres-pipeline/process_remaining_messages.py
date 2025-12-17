#!/usr/bin/env python3
"""
Process Remaining Messages in RabbitMQ Queue
"""

import logging
from production_cash_pipeline import ProductionCashPipeline

def process_remaining_messages():
    """Process all remaining messages in the cash_information_queue"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🧹 Processing Remaining Messages in Queue")
    logger.info("=" * 50)
    
    # Initialize production pipeline (no fetching, just consuming)
    pipeline = ProductionCashPipeline()
    
    # Setup queue
    pipeline.setup_rabbitmq_queue()
    
    # Process all remaining messages
    logger.info("🔄 Processing all remaining messages...")
    processed_count = pipeline.consume_all_messages()
    
    logger.info(f"✅ Completed! Processed {processed_count} remaining messages")
    
    # Show final PostgreSQL count
    try:
        with pipeline.get_postgres_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM cash_information")
            total_count = cursor.fetchone()[0]
            logger.info(f"💾 Total records in PostgreSQL: {total_count:,}")
    except Exception as e:
        logger.error(f"❌ PostgreSQL check error: {e}")

if __name__ == "__main__":
    process_remaining_messages()