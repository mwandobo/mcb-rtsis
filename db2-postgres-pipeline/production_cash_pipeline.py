#!/usr/bin/env python3
"""
Production Cash Pipeline - No Timeouts, Processes All Messages
"""

import pika
import psycopg2
from db2_connection import DB2Connection
import json
import logging
from datetime import datetime
from contextlib import contextmanager
from dataclasses import asdict

from config import Config
from processors.cash_processor import CashProcessor, CashRecord
from pipeline_tracker import PipelineTracker

class ProductionCashPipeline:
    def __init__(self, manual_start_timestamp=None, limit=1000):
        """
        Production Cash Pipeline - Processes ALL messages without timeouts
        
        Args:
            manual_start_timestamp (str): Manual start timestamp in 'YYYY-MM-DD HH:MM:SS' format
            limit (int): Number of records to fetch per run
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.tracker = PipelineTracker()
        self.manual_start_timestamp = manual_start_timestamp
        self.limit = limit
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.cash_processor = CashProcessor()
        
        self.logger.info("🏭 Production Cash Pipeline initialized")
        if manual_start_timestamp:
            self.logger.info(f"🔧 Manual start timestamp: {manual_start_timestamp}")
        self.logger.info(f"📊 Record limit: {limit}")
        
    @contextmanager
    def get_db2_connection(self):
        """Get DB2 connection"""
        with self.db2_conn.get_connection() as conn:
            yield conn
            
    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            yield conn
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for cash"""
        try:
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Declare cash queue
            channel.queue_declare(queue='cash_information_queue', durable=True)
            
            connection.close()
            self.logger.info("✅ RabbitMQ cash queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def fetch_and_publish_cash(self):
        """Fetch cash data with professional tracking"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Get incremental query with tracking
                if self.manual_start_timestamp:
                    # Set manual timestamp and use it
                    self.tracker.set_last_processed_timestamp('cash_information', self.manual_start_timestamp)
                    timestamp_filter = f"AND gte.TMSTAMP > TIMESTAMP('{self.manual_start_timestamp}')"
                    self.logger.info(f"🔧 Using manual start: {self.manual_start_timestamp}")
                else:
                    # Use tracking system
                    timestamp_filter = self.tracker.get_incremental_query_filter(
                        'cash_information', 
                        'gte.TMSTAMP', 
                        default_lookback_days=7
                    )
                    self.logger.info(f"📅 Using tracking filter: {timestamp_filter}")
                
                # Build query with tracking using TMSTAMP
                cash_query = f"""
                SELECT 
                    gte.TMSTAMP,
                    gte.TRN_DATE,
                    CURRENT_TIMESTAMP AS REPORTINGDATE,
                    gte.FK_UNITCODETRXUNIT AS BRANCHCODE,
                    CASE 
                        WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                        WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('101000010','101000015') THEN 'Cash in ATMs'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('101000004','101000011') THEN 'Cash in Teller'
                        ELSE 'Other cash'
                    END AS CASHCATEGORY,
                    gte.CURRENCY_SHORT_DES AS CURRENCY,
                    gte.DC_AMOUNT AS ORGAMOUNT,
                    CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT ELSE NULL END AS USDAMOUNT,
                    CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT*2500 ELSE gte.DC_AMOUNT END AS TZSAMOUNT,
                    gte.TRN_DATE AS TRANSACTIONDATE,
                    gte.AVAILABILITY_DATE AS MATURITYDATE,
                    CAST(0 AS DECIMAL(18,2)) AS ALLOWANCEPROBABLELOSS,
                    CAST(0 AS DECIMAL(18,2)) AS BOTPROVISSION
                FROM GLI_TRX_EXTRACT gte 
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
                {timestamp_filter}
                ORDER BY gte.TMSTAMP ASC
                FETCH FIRST {self.limit} ROWS ONLY
                """
                
                self.logger.info(f"📊 Executing cash query...")
                cursor.execute(cash_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"💰 Fetched {len(rows)} new cash records")
                
                if not rows:
                    self.logger.info("ℹ️ No new cash records found")
                    return 0, None
                
                # Show timestamp range (TMSTAMP is now first column)
                first_timestamp = rows[0][0]
                last_timestamp = rows[-1][0]
                self.logger.info(f"📅 Timestamp range: {first_timestamp} to {last_timestamp}")
                
                # Process and publish (adjust for new column order)
                records = []
                for row in rows:
                    # Skip TMSTAMP column for processor (it expects old format)
                    adjusted_row = row[1:]  # Remove TMSTAMP, keep TRN_DATE and rest
                    record = self.cash_processor.process_record(adjusted_row, 'cash_information')
                    if self.cash_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'cash_information_queue')
                    self.logger.info(f"✅ Published {len(records)} cash records to queue")
                    
                    # Update tracking with the latest TMSTAMP
                    self.tracker.set_last_processed_timestamp('cash_information', str(last_timestamp))
                    self.tracker.update_processing_stats('cash_information', len(records))
                
                return len(records), str(last_timestamp)
                
        except Exception as e:
            self.logger.error(f"❌ Cash fetch error: {e}")
            self.tracker.update_processing_stats('cash_information', 0, has_error=True)
            return 0, None
    
    def publish_records(self, records, queue_name):
        """Publish records to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            for record in records:
                message = json.dumps(asdict(record), default=str)
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Publish error to {queue_name}: {e}")
            raise
    
    def consume_all_messages(self):
        """Consume and process ALL messages in the queue - NO TIMEOUTS"""
        self.logger.info("🏭 Starting production consumer - will process ALL messages")
        
        try:
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            processed_count = 0
            
            def process_cash_message(ch, method, properties, body):
                nonlocal processed_count
                try:
                    record_data = json.loads(body)
                    record = CashRecord(**record_data)
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.cash_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    processed_count += 1
                    
                    # Log every 50 records to avoid spam
                    if processed_count % 50 == 0:
                        self.logger.info(f"✅ Processed {processed_count} records...")
                    
                except Exception as e:
                    self.logger.error(f"❌ Processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Check queue status first
            method_frame = channel.queue_declare(queue='cash_information_queue', passive=True)
            message_count = method_frame.method.message_count
            self.logger.info(f"📊 Found {message_count} messages in queue")
            
            if message_count == 0:
                self.logger.info("ℹ️ No messages to process")
                connection.close()
                return 0
            
            channel.basic_consume(
                queue='cash_information_queue',
                on_message_callback=process_cash_message
            )
            
            # Process ALL messages - no timeout!
            self.logger.info("🔄 Processing all messages (no timeout)...")
            
            while True:
                # Check if queue is empty
                method_frame = channel.queue_declare(queue='cash_information_queue', passive=True)
                remaining_messages = method_frame.method.message_count
                
                if remaining_messages == 0:
                    self.logger.info("✅ All messages processed!")
                    break
                
                # Process messages in batches
                connection.process_data_events(time_limit=1)
            
            connection.close()
            self.logger.info(f"🎉 Production consumer completed: {processed_count} records processed")
            return processed_count
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            return 0
    
    def run_production_pipeline(self):
        """Run the complete production cash pipeline"""
        self.logger.info("🏭 Starting Production Cash Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Show current tracking status
            self.tracker.show_all_tracking_info()
            
            # Step 1: Setup infrastructure
            self.setup_rabbitmq_queue()
            
            # Step 2: Fetch and publish with tracking
            self.logger.info("\n📊 Fetching cash data...")
            record_count, last_timestamp = self.fetch_and_publish_cash()
            
            if record_count == 0:
                self.logger.info("✅ No new records to process")
                return
            
            # Step 3: Process ALL records (no timeout)
            self.logger.info(f"\n🔄 Processing ALL {record_count} records...")
            processed_count = self.consume_all_messages()
            
            self.logger.info(f"\n📊 Final Results:")
            self.logger.info(f"   Fetched: {record_count} records")
            self.logger.info(f"   Processed: {processed_count} records")
            
            # Step 4: Show final status
            self.logger.info("\n📊 Final Tracking Status:")
            self.tracker.show_all_tracking_info()
            
            self.logger.info("🎉 Production cash pipeline completed successfully!")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function for production use"""
    
    print("🏭 Production Cash Pipeline - BOT Project")
    print("=" * 50)
    print("⚠️  This will process ALL messages without timeouts")
    print("=" * 50)
    
    # For production, you can set these parameters:
    MANUAL_START = None  # Set to specific timestamp if needed
    RECORD_LIMIT = 1000  # Adjust based on your needs
    
    pipeline = ProductionCashPipeline(
        manual_start_timestamp=MANUAL_START,
        limit=RECORD_LIMIT
    )
    
    pipeline.run_production_pipeline()

if __name__ == "__main__":
    main()