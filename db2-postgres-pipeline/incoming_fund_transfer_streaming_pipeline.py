#!/usr/bin/env python3
"""
Incoming Fund Transfer Streaming Pipeline - Producer and Consumer run simultaneously
"""

import pika
import psycopg2
import json
import logging
import threading
import time
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from db2_connection import DB2Connection
from processors.incoming_fund_transfer_processor import IncomingFundTransferProcessor, IncomingFundTransferRecord

class IncomingFundTransferStreamingPipeline:
    def __init__(self, batch_size=50):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.processor = IncomingFundTransferProcessor()
        self.batch_size = batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        self.total_available = 0
        self.start_time = time.time()
        
        # Retry settings
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Incoming Fund Transfer STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch (optimized with cursor pagination)")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_incoming_fund_transfer_query(self, last_trn_date=None, last_trn_snum=None):
        """Get the incoming fund transfer query with cursor-based pagination for better performance"""
        
        # Use cursor-based pagination instead of ROW_NUMBER() for much better performance
        where_clause = """WHERE GL.EXTERNAL_GLACCOUNT = '100026000' AND wdc.ID_NO IS NOT NULL"""
        
        if last_trn_date and last_trn_snum:
            where_clause += f"""
            AND (gte.TRN_DATE > '{last_trn_date}' 
                 OR (gte.TRN_DATE = '{last_trn_date}' AND gte.TRN_SNUM > {last_trn_snum}))
            """
        
        query = f"""
        SELECT CURRENT_TIMESTAMP              AS reportingDate,
               VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
               TRIM(gte.FK_USRCODE) || '-' ||
               VARCHAR(gte.LINE_NUM) || '-' ||
               VARCHAR(gte.TRN_DATE) || '-' ||
               VARCHAR(gte.TRN_SNUM)          AS transactionId,
               gte.TRN_DATE                   AS transactionDate,
               'EFT'                          AS transferChannel,
               NULL                           AS subCategoryTransferChannel,
               wdc.NAME_STANDARD                           AS recipientName,
               pa.ACCOUNT_NUMBER              AS senderAccountNumber,
               CASE
                   WHEN LENGTH(TRIM(wdc.ID_NO)) > 18 THEN 'NationalIdentityCard'
                   WHEN LENGTH(TRIM(wdc.ID_NO)) BETWEEN 9 AND 11 AND TRANSLATE(
                                                                             REPLACE(TRIM(wdc.ID_NO), '-', ''),
                                                                             '',
                                                                             '0123456789T'
                                                                     ) = '' THEN 'DrivingLicense'
                   WHEN wdc.ID_NO LIKE 'T%' AND TRANSLATE(
                                                        REPLACE(TRIM(wdc.ID_NO), '-', ''),
                                                        '',
                                                        '0123456789T'
                                                ) = '' AND LENGTH(TRIM(wdc.ID_NO)) > 10 THEN 'VotersRegistrationCard'
                   WHEN wdc.ID_NO LIKE 'AB%' THEN 'Passport'
                   ELSE 'Employee ID'
                   END                        AS recipientIdentificationType,
               wdc.ID_NO                      AS recipientIdentificationNumber,
               'TANZANIA, UNITED REPUBLIC OF' AS recipientCountry,
               wdc.NAME_STANDARD              AS senderName,
               NULL                           AS senderBankOrFspCode,
               NULL                           AS senderAccountOrWalletNumber,
               NULL                           AS serviceCategory,
               NULL                           AS serviceSubCategory,
               gte.CURRENCY_SHORT_DES         AS currency,
               gte.DC_AMOUNT                  AS orgAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
                   ELSE DECIMAL(gte.DC_AMOUNT / 2500, 18, 2) -- USD conversion
                   END                        AS usdAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN DECIMAL(gte.DC_AMOUNT, 18, 0)
                   ELSE DECIMAL(gte.DC_AMOUNT * 2500, 18, 0) -- TZS conversion
                   END                        AS tzsAmount,
               NULL                           AS purposes,
               NULL                           AS senderInstruction,
               
               gte.TRN_DATE as cursor_trn_date,
               gte.TRN_SNUM as cursor_trn_snum

        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN (SELECT *
                            FROM (SELECT wdc.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                                  FROM W_DIM_CUSTOMER wdc)
                            WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
                 LEFT JOIN (SELECT *
                            FROM (SELECT pa.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                                  FROM PROFITS_ACCOUNT pa
                                  WHERE PRFT_SYSTEM = 3)
                            WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
                 LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
        {where_clause}
        ORDER BY gte.TRN_DATE ASC, gte.TRN_SNUM ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available incoming fund transfer records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN (SELECT *
                            FROM (SELECT wdc.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                                  FROM W_DIM_CUSTOMER wdc)
                            WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
                 LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
        WHERE GL.EXTERNAL_GLACCOUNT = '100026000' AND wdc.ID_NO IS NOT NULL
        """
    
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
    
    def setup_rabbitmq_connection(self, max_retries=3):
        """Setup RabbitMQ connection with retry logic"""
        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(
                    self.config.message_queue.rabbitmq_user,
                    self.config.message_queue.rabbitmq_password
                )
                parameters = pika.ConnectionParameters(
                    host=self.config.message_queue.rabbitmq_host,
                    port=self.config.message_queue.rabbitmq_port,
                    credentials=credentials,
                    heartbeat=600,  # 10 minutes heartbeat
                    blocked_connection_timeout=300,  # 5 minutes timeout
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                return connection, channel
                
            except Exception as e:
                self.logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"Failed to connect to RabbitMQ after {max_retries} attempts")
                    raise
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for incoming fund transfer"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare queue with durability
            channel.queue_declare(queue='incoming_fund_transfer_queue', durable=True)
            
            connection.close()
            self.logger.info("RabbitMQ queue 'incoming_fund_transfer_queue' setup complete")
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ with cursor-based pagination"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total incoming fund transfer records available: {self.total_available:,}")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches using cursor-based pagination
            batch_number = 1
            last_trn_date = None
            last_trn_snum = None
            last_progress_report = time.time()
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch with retry logic using cursor pagination
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection() as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_incoming_fund_transfer_query(last_trn_date, last_trn_snum)
                            cursor.execute(batch_query)
                            rows = cursor.fetchall()
                        break
                    except Exception as e:
                        self.logger.warning(f"DB2 query attempt {attempt + 1} failed: {e}")
                        if attempt < self.max_retries - 1:
                            time.sleep(self.retry_delay)
                        else:
                            raise
                
                if not rows:
                    self.logger.info("No more records to process")
                    break
                
                # Process and publish with retry logic
                batch_published = 0
                for row in rows:
                    # Extract cursor values for next batch (last two fields)
                    last_trn_date = row[-2]  # cursor_trn_date
                    last_trn_snum = row[-1]  # cursor_trn_snum
                    
                    # Remove cursor fields before processing
                    row_without_cursor = row[:-2]
                    record = self.processor.process_record(row_without_cursor, 'incoming_fund_transfer')
                    
                    if self.processor.validate_record(record):
                        message = json.dumps(asdict(record), default=str)
                        
                        # Publish with retry
                        published = False
                        for attempt in range(self.max_retries):
                            try:
                                channel.basic_publish(
                                    exchange='',
                                    routing_key='incoming_fund_transfer_queue',
                                    body=message,
                                    properties=pika.BasicProperties(delivery_mode=2)
                                )
                                published = True
                                break
                            except Exception as e:
                                self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                                if attempt < self.max_retries - 1:
                                    # Reconnect and retry
                                    try:
                                        connection.close()
                                    except:
                                        pass
                                    connection, channel = self.setup_rabbitmq_connection()
                                    time.sleep(self.retry_delay)
                                else:
                                    self.logger.error(f"Failed to publish message after {self.max_retries} attempts")
                        
                        if published:
                            batch_published += 1
                            self.total_produced += 1
                
                batch_time = time.time() - batch_start_time
                progress_percent = self.total_produced / self.total_available * 100 if self.total_available > 0 else 0
                
                self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                
                # Progress report every 5 minutes
                current_time = time.time()
                if current_time - last_progress_report >= 300:  # 5 minutes
                    elapsed_time = current_time - self.start_time
                    rate = self.total_produced / elapsed_time if elapsed_time > 0 else 0
                    remaining_records = self.total_available - self.total_produced
                    eta_seconds = remaining_records / rate if rate > 0 else 0
                    eta_hours = eta_seconds / 3600
                    
                    self.logger.info(f"PROGRESS REPORT: {self.total_produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                    last_progress_report = current_time
                
                # Move to next batch
                batch_number += 1
                
                # Small delay to prevent overwhelming
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced:,} records published out of {self.total_available:,} available")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue with retry logic"""
        try:
            self.logger.info("Consumer thread started")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Initialize progress tracking variable
            last_progress_report = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal last_progress_report  # Allow modification of the outer variable
                try:
                    record_data = json.loads(body)
                    record = IncomingFundTransferRecord(**record_data)
                    
                    # Insert to PostgreSQL with retry
                    inserted = False
                    for attempt in range(self.max_retries):
                        try:
                            with self.get_postgres_connection() as conn:
                                cursor = conn.cursor()
                                self.processor.insert_to_postgres(record, cursor)
                                conn.commit()
                            inserted = True
                            break
                        except Exception as e:
                            self.logger.warning(f"PostgreSQL insert attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                            else:
                                self.logger.error(f"Failed to insert record after {self.max_retries} attempts")
                    
                    if inserted:
                        self.total_consumed += 1
                        
                        # Progress monitoring
                        if self.total_consumed % (self.batch_size * 2) == 0:  # Every 2 batches
                            elapsed_time = time.time() - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                            
                            self.logger.info(f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}% of total) - Rate: {rate:.1f} rec/sec")
                        
                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:  # 5 minutes
                            elapsed_time = current_time - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            remaining_records = self.total_available - self.total_consumed if self.total_available > 0 else 0
                            eta_seconds = remaining_records / rate if rate > 0 else 0
                            eta_hours = eta_seconds / 3600
                            
                            self.logger.info(f"CONSUMER PROGRESS: {self.total_consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                            last_progress_report = current_time
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue='incoming_fund_transfer_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='incoming_fund_transfer_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect
                    try:
                        connection.close()
                    except:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=10)
                    channel.basic_consume(queue='incoming_fund_transfer_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Incoming Fund Transfer STREAMING pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()
            
            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("Producer thread completed")
            
            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=60)  # Wait up to 60 seconds
            
            if consumer_thread.is_alive():
                self.logger.info("Stopping consumer thread...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)
            
            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Incoming Fund Transfer Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/3600:.2f} hours
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Incoming Fund Transfer Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = IncomingFundTransferStreamingPipeline(batch_size=args.batch_size)
    
    try:
        if args.mode == 'producer':
            pipeline.producer_thread()
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:  # streaming
            pipeline.run_streaming_pipeline()
            
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()