#!/usr/bin/env python3
"""
Card Transactions Streaming Pipeline - Producer and Consumer run simultaneously
Uses card-transaction-v1.sql query for comprehensive card transaction data extraction
"""

import pika
import psycopg2
import json
import logging
import threading
import time
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from typing import Optional
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection


@dataclass
class CardTransactionRecord:
    """Data class for card transaction records based on card-transaction-v1.sql"""
    reportingDate: str
    cardNumber: str
    binNumber: str
    transactingBankName: str
    transactionId: str
    transactionDate: str
    transactionNature: str
    atmCode: Optional[str]
    posNumber: Optional[str]
    transactionDescription: Optional[str]
    beneficiaryName: Optional[str]
    beneficiaryTradeName: Optional[str]
    beneficaryCountry: str
    transactionPlace: str
    qtyItemsPurchased: Optional[str]
    unitPrice: Optional[str]
    orgFacilitatorCommissionAmount: Optional[float]
    usdFacilitatorCommissionAmount: Optional[float]
    tzsFacilitatorCommissionAmount: Optional[float]
    currency: str
    orgTransactionAmount: Optional[float]
    usdTransactionAmount: Optional[float]
    tzsTransactionAmount: Optional[float]


class CardTransactionsStreamingPipeline:
    def __init__(self, batch_size=1000):
        self.config = Config()
        self.db2_conn = DB2Connection()
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
        
        self.logger.info("Card Transactions STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_card_transactions_query(self, last_row_number=None):
        """Get the card transactions query using card-transaction-v1.sql with cursor-based pagination"""
        
        # Read the card-transaction-v1.sql file
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'sqls', 'card-transaction-v1.sql')
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            base_query = f.read()
        
        # Build cursor-based pagination condition using ROW_NUMBER
        cursor_condition = ""
        if last_row_number:
            cursor_condition = f" WHERE ce.rn > {last_row_number}"
        
        # Insert cursor condition before the final ORDER BY
        if cursor_condition:
            # Add WHERE clause after the FROM clause and before any existing WHERE
            base_query = base_query.replace(
                "FROM ce_numbered ce",
                f"FROM ce_numbered ce{cursor_condition}"
            )
        
        # Add ORDER BY and FETCH FIRST for pagination
        query = base_query.rstrip(';') + f"""
        ORDER BY ce.rn ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available card transaction records"""
        return """
        SELECT COUNT(*) as total_count
        FROM CMS_CARD_EXTRAIT ce
        LEFT JOIN CMS_CARD ca ON ca.CARD_SN = ce.CARD_SN
        LEFT JOIN (SELECT ISO_CODE, MIN(DESCRIPTION) AS DESCRIPTION
                   FROM ATM_PROCESS_CODE
                   GROUP BY ISO_CODE) pc ON pc.ISO_CODE = ce.PROCESS_CD
        JOIN CMS_CARD_ACCOUNT card_acc ON ce.CARD_SN = card_acc.FK_CARD_SN
        JOIN PROFITS_ACCOUNT pa ON pa.PRFT_SYSTEM = card_acc.PRFT_SYSTEM
        JOIN CURRENCY curr ON curr.ID_CURRENCY = pa.MOVEMENT_CURRENCY
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
        """Setup RabbitMQ queue for card transactions"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare queue with durability
            channel.queue_declare(queue='card_transactions_queue', durable=True)
            
            connection.close()
            self.logger.info("RabbitMQ queue 'card_transactions_queue' setup complete")
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single card transaction record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            def safe_float(value):
                """Safely convert to float"""
                if value is None:
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            # Map the fields from the SQL query to the dataclass
            # Based on the exact field order in card-transaction-v1.sql
            record = CardTransactionRecord(
                reportingDate=safe_string(row[0]),                    # VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')
                cardNumber=safe_string(row[1]),                       # ca.FULL_CARD_NO
                binNumber=safe_string(row[2]),                        # RIGHT(TRIM(ca.FULL_CARD_NO), 10)
                transactingBankName=safe_string(row[3]),              # 'Mwalimu Commercial Bank Plc'
                transactionId=safe_string(row[4]),                    # Composite ID
                transactionDate=safe_string(row[5]),                  # VARCHAR_FORMAT(ce.TUN_DATE, 'DDMMYYYYHHMM')
                transactionNature=safe_string(row[6]),                # 'Local Transactions by Locally Issued Cards'
                atmCode=safe_string(row[7]),                          # null
                posNumber=safe_string(row[8]),                        # null
                transactionDescription=safe_string(row[9]),           # pc.DESCRIPTION
                beneficiaryName=safe_string(row[10]),                 # ca.CARD_NAME_LATIN
                beneficiaryTradeName=safe_string(row[11]),            # null
                beneficaryCountry=safe_string(row[12]),               # 'TANZANIA, UNITED REPUBLIC OF'
                transactionPlace=safe_string(row[13]),                # 'TANZANIA, UNITED REPUBLIC OF'
                qtyItemsPurchased=safe_string(row[14]),               # null
                unitPrice=safe_string(row[15]),                       # null
                orgFacilitatorCommissionAmount=safe_float(row[16]),   # null
                usdFacilitatorCommissionAmount=safe_float(row[17]),   # null
                tzsFacilitatorCommissionAmount=safe_float(row[18]),   # null
                currency=safe_string(row[19]),                        # curr.SHORT_DESCR
                orgTransactionAmount=safe_float(row[20]),             # ce.TRANSACTION_AMNT
                usdTransactionAmount=safe_float(row[21]),             # Currency conversion logic
                tzsTransactionAmount=safe_float(row[22])              # Currency conversion logic
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing card transaction record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate card transaction record"""
        try:
            # Basic validation
            if not record.transactionId:
                self.logger.warning("Missing transaction ID")
                return False
            
            if not record.cardNumber:
                self.logger.warning("Missing card number")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating card transaction record: {e}")
            return False
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ with cursor-based pagination"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total card transaction records available: {self.total_available:,}")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches using cursor-based pagination
            batch_number = 1
            last_row_number = None
            last_progress_report = time.time()
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch with retry logic using cursor pagination
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_card_transactions_query(last_row_number)
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
                    # Extract cursor value - we need to get the row number from the CTE
                    # Since the row number is generated in the CTE, we'll track it manually
                    if last_row_number is None:
                        last_row_number = 0
                    last_row_number += 1
                    
                    # Process the full row
                    record = self.process_record(row)
                    
                    if self.validate_record(record):
                        message = json.dumps(asdict(record), default=str)
                        
                        # Publish with retry
                        published = False
                        for attempt in range(self.max_retries):
                            try:
                                channel.basic_publish(
                                    exchange='',
                                    routing_key='card_transactions_queue',
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
                    record = CardTransactionRecord(**record_data)
                    
                    # Insert to PostgreSQL with retry
                    inserted = False
                    for attempt in range(self.max_retries):
                        try:
                            with self.get_postgres_connection() as conn:
                                cursor = conn.cursor()
                                self.insert_to_postgres(record, cursor)
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
                        if self.total_consumed % (self.batch_size // 10) == 0:  # Every 10% of batch size
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
            channel.basic_consume(queue='card_transactions_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='card_transactions_queue', durable=True, passive=True)
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
                    channel.basic_consume(queue='card_transactions_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def insert_to_postgres(self, record, cursor):
        """Insert card transaction record to PostgreSQL"""
        try:
            insert_query = """
            INSERT INTO "cardTransactions" (
                "reportingDate", "cardNumber", "binNumber", "transactingBankName", "transactionId",
                "transactionDate", "transactionNature", "atmCode", "posNumber", "transactionDescription",
                "beneficiaryName", "beneficiaryTradeName", "beneficaryCountry", "transactionPlace",
                "qtyItemsPurchased", "unitPrice", "orgFacilitatorCommissionAmount", "usdFacilitatorCommissionAmount",
                "tzsFacilitatorCommissionAmount", "currency", "orgTransactionAmount", "usdTransactionAmount",
                "tzsTransactionAmount"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate,
                record.cardNumber,
                record.binNumber,
                record.transactingBankName,
                record.transactionId,
                record.transactionDate,
                record.transactionNature,
                record.atmCode,
                record.posNumber,
                record.transactionDescription,
                record.beneficiaryName,
                record.beneficiaryTradeName,
                record.beneficaryCountry,
                record.transactionPlace,
                record.qtyItemsPurchased,
                record.unitPrice,
                record.orgFacilitatorCommissionAmount,
                record.usdFacilitatorCommissionAmount,
                record.tzsFacilitatorCommissionAmount,
                record.currency,
                record.orgTransactionAmount,
                record.usdTransactionAmount,
                record.tzsTransactionAmount
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting card transaction record to PostgreSQL: {e}")
            raise
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Card Transactions STREAMING pipeline...")
        
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
            Card Transactions Pipeline Summary:
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
    
    parser = argparse.ArgumentParser(description='Card Transactions Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = CardTransactionsStreamingPipeline(batch_size=args.batch_size)
    
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