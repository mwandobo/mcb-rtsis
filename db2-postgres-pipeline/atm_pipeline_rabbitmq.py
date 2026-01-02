#!/usr/bin/env python3
"""
Professional ATM Pipeline with RabbitMQ and Tracking - BOT Project
"""

import pika
import psycopg2
from db2_connection import DB2Connection
import json
import time
import logging
import threading
from datetime import datetime
from contextlib import contextmanager
from dataclasses import asdict

from config import Config
from processors.atm_processor import AtmProcessor, AtmRecord
from pipeline_tracker import PipelineTracker

class ProfessionalAtmPipeline:
    def __init__(self, manual_start_timestamp=None, limit=1000):
        """
        Professional ATM Pipeline with RabbitMQ and Tracking
        
        Args:
            manual_start_timestamp (str): Manual start timestamp in 'YYYY-MM-DD HH:MM:SS' format
            limit (int): Number of records to fetch per run
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.tracker = PipelineTracker()
        self.running = True
        self.manual_start_timestamp = manual_start_timestamp
        self.limit = limit
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.atm_processor = AtmProcessor()
        
        self.logger.info("🏧 Professional ATM Pipeline initialized")
        if manual_start_timestamp:
            self.logger.info(f"🔧 Manual start timestamp: {manual_start_timestamp}")
        self.logger.info(f"📊 Record limit: {limit}")
        
    def get_tracked_atm_query(self):
        """Get ATM query with professional tracking"""
        
        # Check if manual start timestamp is provided
        if self.manual_start_timestamp:
            # Set manual timestamp and use it
            self.tracker.set_last_processed_timestamp('atm_information', self.manual_start_timestamp)
            timestamp_filter = f"AND COALESCE(gte.TRN_DATE, at.INSTALLATION_DATE) > TIMESTAMP('{self.manual_start_timestamp}')"
            self.logger.info(f"🔧 Using manual start: {self.manual_start_timestamp}")
        else:
            # Use tracking system
            timestamp_filter = self.tracker.get_incremental_query_filter(
                'atm_information', 
                'COALESCE(gte.TRN_DATE, at.INSTALLATION_DATE)', 
                default_lookback_days=7
            )
            self.logger.info(f"📅 Using tracking filter: {timestamp_filter}")
        
        query = f"""
        SELECT
            CURRENT_TIMESTAMP AS reportingDate,
            'ATM' || CAST(at.TERMINAL_ID AS VARCHAR(10)) AS atmId,
            COALESCE(at.TERMINAL_NAME, 'ATM Terminal ' || CAST(at.TERMINAL_ID AS VARCHAR(10))) AS atmName,
            CAST(at.FK_UNITCODE AS VARCHAR(10)) AS branchCode,
            COALESCE(at.LOCATION, 'Unknown Location') AS atmLocation,
            CASE 
                WHEN at.LOCATION LIKE '%DSM%' OR at.LOCATION LIKE '%DAR%' THEN 'Dar es Salaam'
                WHEN at.LOCATION LIKE '%MWANZA%' THEN 'Mwanza'
                WHEN at.LOCATION LIKE '%MBEYA%' THEN 'Mbeya'
                WHEN at.LOCATION LIKE '%MOROGORO%' THEN 'Morogoro'
                WHEN at.LOCATION LIKE '%ARUSHA%' THEN 'Arusha'
                ELSE 'Dar es Salaam'
            END AS region,
            CASE 
                WHEN at.LOCATION LIKE '%KINONDONI%' THEN 'Kinondoni'
                WHEN at.LOCATION LIKE '%TEMEKE%' THEN 'Temeke'
                WHEN at.LOCATION LIKE '%ILALA%' THEN 'Ilala'
                WHEN at.LOCATION LIKE '%UBUNGO%' THEN 'Ubungo'
                ELSE 'Kinondoni'
            END AS district,
            CASE 
                WHEN at.LOCATION LIKE '%MSASANI%' THEN 'Msasani'
                WHEN at.LOCATION LIKE '%MAGOMENI%' THEN 'Magomeni'
                WHEN at.LOCATION LIKE '%KARIAKOO%' THEN 'Kariakoo'
                ELSE 'Msasani'
            END AS ward,
            COALESCE(SUBSTR(at.LOCATION, 1, 100), 'Unknown Street') AS street,
            '0.0000,0.0000' AS gpsCoordinates,
            CASE 
                WHEN at.TERMINAL_TYPE = 'ATM' THEN 'Cash Dispenser'
                WHEN at.TERMINAL_TYPE = 'CDM' THEN 'Cash Deposit Machine'
                ELSE 'Multi-Function ATM'
            END AS atmType,
            CASE 
                WHEN at.ENTRY_STATUS = '1' THEN 'Active'
                WHEN at.ENTRY_STATUS = '0' THEN 'Inactive'
                ELSE 'Maintenance'
            END AS atmStatus,
            COALESCE(at.INSTALLATION_DATE, CURRENT_DATE - 365 DAYS) AS installationDate,
            at.LAST_MAINTENANCE_DATE AS lastMaintenanceDate,
            'TZS' AS currency,
            COALESCE(gte.DC_AMOUNT, 0) AS cashLoadedAmount,
            CASE 
                WHEN 'TZS' = 'USD' THEN COALESCE(gte.DC_AMOUNT, 0)
                ELSE NULL
            END AS usdCashLoaded,
            COALESCE(gte.DC_AMOUNT, 0) AS tzsCashLoaded,
            COALESCE(gte.CR_AMOUNT, 0) AS cashDispensedAmount,
            CASE 
                WHEN 'TZS' = 'USD' THEN COALESCE(gte.CR_AMOUNT, 0)
                ELSE NULL
            END AS usdCashDispensed,
            COALESCE(gte.CR_AMOUNT, 0) AS tzsCashDispensed,
            COALESCE(COUNT(gte.TRN_DATE), 0) AS transactionCount,
            COALESCE(gte.TRN_DATE, at.INSTALLATION_DATE) AS transactionDate,
            0 AS allowanceProbableLoss,
            0 AS botProvision
        FROM AGENT_TERMINAL at
        LEFT JOIN GLI_TRX_EXTRACT gte ON gte.FK_UNITCODETRXUNIT = at.FK_UNITCODE 
            AND gte.TRN_DATE >= CURRENT_DATE - 30 DAYS
            AND gte.FK_GLG_ACCOUNTACCO IN (
                SELECT ACCOUNT_ID FROM GLG_ACCOUNT 
                WHERE EXTERNAL_GLACCOUNT IN ('101000010', '101000015')
            )
        WHERE at.TERMINAL_TYPE IN ('ATM', 'CDM', 'MULTI')
        {timestamp_filter}
        GROUP BY at.TERMINAL_ID, at.TERMINAL_NAME, at.FK_UNITCODE, at.LOCATION, 
                 at.TERMINAL_TYPE, at.ENTRY_STATUS, at.INSTALLATION_DATE, 
                 at.LAST_MAINTENANCE_DATE, gte.DC_AMOUNT, gte.CR_AMOUNT, gte.TRN_DATE
        ORDER BY COALESCE(gte.TRN_DATE, at.INSTALLATION_DATE) ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
        
        return query
        
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
        """Setup RabbitMQ queue for ATM"""
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
            
            # Declare ATM queue
            channel.queue_declare(queue='atm_information_queue', durable=True)
            
            connection.close()
            self.logger.info("✅ RabbitMQ ATM queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def fetch_and_publish_atm(self):
        """Fetch ATM data with professional tracking"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                atm_query = self.get_tracked_atm_query()
                self.logger.info("📊 Executing tracked ATM query...")
                
                cursor.execute(atm_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"🏧 Fetched {len(rows)} ATM records")
                
                if not rows:
                    self.logger.info("ℹ️ No new ATM records found (tracking prevents duplicates)")
                    return 0, None
                
                # Show timestamp range of fetched data
                first_timestamp = rows[0][15]  # openingDate is at index 15
                last_timestamp = rows[-1][15]
                self.logger.info(f"📅 Processing timestamp range: {first_timestamp} to {last_timestamp}")
                
                # Show sample data
                self.logger.info("📋 Sample ATM records:")
                for i, row in enumerate(rows[:3], 1):
                    # row structure: reportingDate, atmName, branchCode, atmCode, tillNumber, mobileMoneyServices, qrFsrCode, postalCode, region, district, ward, street, houseNumber, gpsCoordinates, linkedAccount, openingDate, atmStatus, closureDate, atmChannel
                    self.logger.info(f"  {i}. {row[15]} | ATM: {row[3]} ({row[1]}) | Branch: {row[2]} | Status: {row[16]} | Account: {row[14]}")
                
                # Process and publish (skip reportingDate for processor - it expects the 18 fields from atmName through atmChannel)
                records = []
                for row in rows:
                    # Skip reportingDate column for processor - it expects the 18 fields: atmName through atmChannel
                    adjusted_row = row[1:]  # Remove reportingDate, keep the 18 fields: atmName through atmChannel
                    record = self.atm_processor.process_record(adjusted_row, 'atm_information')
                    if self.atm_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'atm_information_queue')
                    self.logger.info(f"✅ Published {len(records)} ATM records to queue")
                
                return len(records), str(last_timestamp)
                
        except Exception as e:
            self.logger.error(f"❌ ATM fetch error: {e}")
            self.tracker.update_processing_stats('atm_information', 0, has_error=True)
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
    
    def consume_and_process_atm(self):
        """Consume and process ATM records with tracking"""
        self.logger.info("🔄 Starting professional ATM consumer...")
        
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
            
            def process_atm_message(ch, method, properties, body):
                nonlocal processed_count
                try:
                    record_data = json.loads(body)
                    record = AtmRecord(**record_data)
                    
                    self.logger.info(f"🏧 Processing: {record.opening_date} | ATM {record.atm_code} ({record.atm_name}) | Branch {record.branch_code} | Status: {record.atm_status} | Account: {record.linked_account}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.atm_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    processed_count += 1
                    self.logger.info(f"✅ Record {processed_count} inserted successfully")
                    
                except Exception as e:
                    self.logger.error(f"❌ Processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='atm_information_queue',
                on_message_callback=process_atm_message
            )
            
            # Process messages for a reasonable time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:
                connection.process_data_events(time_limit=1)
            
            connection.close()
            return processed_count
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            return 0
    
    def run_professional_pipeline(self):
        """Run the complete professional ATM pipeline"""
        self.logger.info("🚀 Starting Professional ATM Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # If manual start timestamp is provided, reset tracking first
            if self.manual_start_timestamp:
                self.logger.info(f"🔧 Manual start detected: {self.manual_start_timestamp}")
                self.logger.info("🔄 Resetting tracking to manual start timestamp...")
                self.tracker.set_last_processed_timestamp('atm_information', self.manual_start_timestamp)
            
            # Show current tracking status
            self.tracker.show_all_tracking_info()
            
            # Step 1: Setup infrastructure
            self.setup_rabbitmq_queue()
            
            # Step 2: Process all data in batches
            total_processed = 0
            batch_number = 1
            
            while True:
                self.logger.info(f"\n📊 Batch {batch_number}: Fetching ATM data with tracking...")
                record_count, last_timestamp = self.fetch_and_publish_atm()
                
                if record_count == 0:
                    self.logger.info("✅ No more records to process - all data processed!")
                    break
                
                # Step 3: Process records
                self.logger.info(f"🔄 Processing {record_count} records from batch {batch_number}...")
                consumer_thread = threading.Thread(target=self.consume_and_process_atm, daemon=True)
                consumer_thread.start()
                
                # Wait for processing (adjust time based on batch size)
                processing_time = max(35, record_count // 10)  # At least 35 seconds, more for larger batches
                time.sleep(processing_time)
                self.running = False
                consumer_thread.join(timeout=10)
                
                # Step 4: Update tracking
                if last_timestamp:
                    self.tracker.set_last_processed_timestamp('atm_information', last_timestamp)
                    self.tracker.update_processing_stats('atm_information', record_count)
                    self.logger.info(f"✅ Batch {batch_number} completed: {record_count} records, last timestamp = {last_timestamp}")
                
                total_processed += record_count
                batch_number += 1
                
                # Reset running flag for next batch
                self.running = True
                
                # Small delay between batches
                time.sleep(2)
            
            # Step 5: Show final status
            self.logger.info(f"\n🎉 ALL ATM DATA PROCESSED SUCCESSFULLY!")
            self.logger.info(f"📊 Total records processed: {total_processed}")
            self.logger.info(f"📊 Total batches: {batch_number - 1}")
            self.logger.info("\n📊 Final Tracking Status:")
            self.tracker.show_all_tracking_info()
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function - Configure your parameters here"""
    
    # ============================================
    # 🔧 PROFESSIONAL CONFIGURATION
    # ============================================
    
    # Set manual start timestamp (or None for automatic tracking)
    MANUAL_START = '2024-12-15 00:00:00'  # Starting from December 15, 2024 (recent test)
    
    # Record limit per run (set to high number for all data, or None for unlimited)
    RECORD_LIMIT = 10000  # Process up to 10,000 records per run
    
    # ============================================
    
    print("🏧 Professional ATM Pipeline - BOT Project")
    print("=" * 50)
    print(f"🔧 Manual Start: {MANUAL_START or 'Automatic tracking'}")
    print(f"📊 Record Limit: {RECORD_LIMIT}")
    print("=" * 50)
    
    pipeline = ProfessionalAtmPipeline(
        manual_start_timestamp=MANUAL_START,
        limit=RECORD_LIMIT
    )
    
    pipeline.run_professional_pipeline()

if __name__ == "__main__":
    main()