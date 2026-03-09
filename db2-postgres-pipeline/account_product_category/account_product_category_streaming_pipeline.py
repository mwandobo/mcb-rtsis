#!/usr/bin/env python3
"""
Account Product Category Streaming Pipeline - Producer and Consumer run simultaneously
Based on account-product-category.sql query
"""

import pika
import psycopg2
import psycopg2.extras
import json
import logging
import threading
import time
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from typing import Optional, List
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection

# Configure logging at module level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class AccountProductCategoryRecord:
    """Data class for account product category records based on account-product-category.sql"""
    reportingDate: str
    accountProductCode: str
    accountProductName: str
    accountProductDescription: str
    accountProductType: str
    accountProductSubType: Optional[str]
    currency: str
    accountProductCreationDate: str
    accountProductClosureDate: Optional[str]
    accountProductStatus: str


class AccountProductCategoryStreamingPipeline:
    def __init__(self, batch_size=1000, consumer_batch_size=100):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = consumer_batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Thread-safe statistics
        self._stats_lock = threading.Lock()
        self.total_produced = 0
        self.total_consumed = 0
        self.total_available = 0
        self.start_time = time.time()
        
        # Retry settings
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Account Product Category STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_account_product_category_query(self):
        """Get the account product category query from account-product-category.sql"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'account-product-category.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def get_total_count(self):
        """Get approximate total count of account product category records from DB2"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM W_DIM_PRODUCT wp
                    LEFT JOIN PRODUCT p ON p.ID_PRODUCT = wp.PRODUCT_CODE
                    WHERE wp.TREE_LEVEL_1 NOT IN ('Agreement', 'Service', 'Collateral')
                      AND wp.TREE_LEVEL_2 NOT IN ('DUMMY', 'HIDDEN ACCOUNT')
                """)
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Estimated record count: {count:,}")
                return count
        except Exception as e:
            self.logger.warning(f"Could not fetch record count, progress % unavailable: {e}")
            return 0
    
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
                    heartbeat=600,
                    blocked_connection_timeout=300,
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
        """Setup RabbitMQ queue for account product category with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='account_product_category_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='account_product_category_dead_letter', durable=True)
            channel.queue_bind(
                queue='account_product_category_dead_letter',
                exchange='account_product_category_dlx',
                routing_key='account_product_category_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='account_product_category_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'account_product_category_dlx',
                        'x-dead-letter-routing-key': 'account_product_category_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                self.logger.warning("Queue 'account_product_category_queue' already exists with different args.")
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='account_product_category_queue', durable=True)
                self.logger.info("RabbitMQ queue 'account_product_category_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single account product category record from DB2"""
        try:
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            # Map the fields from the SQL query to the dataclass
            record = AccountProductCategoryRecord(
                reportingDate=safe_string(row[0]),           # VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')
                accountProductCode=safe_string(row[1]),      # PRODUCT_CODE || currency
                accountProductName=safe_string(row[2]),      # TRIM(wp.DESCRIPTION)
                accountProductDescription=safe_string(row[3]), # TRIM(wp.DESCRIPTION)
                accountProductType=safe_string(row[4]),      # CASE wp.TREE_LEVEL_1
                accountProductSubType=safe_string(row[5]) if row[5] else None, # CASE wp.TREE_LEVEL_2
                currency=safe_string(row[6]),                # Currency from product name
                accountProductCreationDate=safe_string(row[7]), # COALESCE dates
                accountProductClosureDate=safe_string(row[8]) if row[8] else None, # CASE p.ENTRY_STATUS
                accountProductStatus=safe_string(row[9]),    # CASE p.ENTRY_STATUS
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing account product category record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate account product category record"""
        try:
            if not record.accountProductCode:
                self.logger.warning("Missing accountProductCode")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating account product category record: {e}")
            return False

    
    def producer_thread(self):
        """Producer thread - executes query ONCE and streams results via fetchmany()"""
        try:
            self.logger.info("Producer thread started")
            
            # Get dynamic record count
            self.total_available = self.get_total_count()
            
            self.logger.info(f"Total account product category records available: {self.total_available:,} (estimated)")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()
            
            # Execute the query ONCE and stream results
            query = self.get_account_product_category_query()
            self.logger.info("Executing account product category query (single execution, streaming results)...")
            
            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                db2_cursor = db2_conn.cursor()
                db2_cursor.execute(query)
                self.logger.info("Query executed successfully, streaming results via fetchmany()...")
                
                batch_number = 1
                last_progress_report = time.time()
                
                while True:
                    batch_start_time = time.time()
                    
                    # Fetch next chunk from the already-running query
                    rows = db2_cursor.fetchmany(self.batch_size)
                    
                    if not rows:
                        self.logger.info("No more records to fetch")
                        break
                    
                    # Process and publish
                    batch_published = 0
                    for row in rows:
                        record = self.process_record(row)
                        
                        if self.validate_record(record):
                            message = json.dumps(asdict(record), default=str)
                            
                            # Publish with retry
                            published = False
                            for attempt in range(self.max_retries):
                                try:
                                    channel.basic_publish(
                                        exchange='',
                                        routing_key='account_product_category_queue',
                                        body=message,
                                        properties=pika.BasicProperties(delivery_mode=2)
                                    )
                                    published = True
                                    break
                                except Exception as e:
                                    self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                                    if attempt < self.max_retries - 1:
                                        try:
                                            rmq_connection.close()
                                        except Exception:
                                            pass
                                        rmq_connection, channel = self.setup_rabbitmq_connection()
                                        time.sleep(self.retry_delay)
                                    else:
                                        self.logger.error(f"Failed to publish message after {self.max_retries} attempts")
                            
                            if published:
                                batch_published += 1
                                with self._stats_lock:
                                    self.total_produced += 1
                    
                    batch_time = time.time() - batch_start_time
                    with self._stats_lock:
                        produced = self.total_produced
                    progress_percent = produced / self.total_available * 100 if self.total_available > 0 else 0
                    
                    self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} rows fetched, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                    
                    # Progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        rate = produced / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - produced
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"PROGRESS REPORT: {produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                    batch_number += 1
            
            rmq_connection.close()
            with self._stats_lock:
                produced = self.total_produced
            self.logger.info(f"Producer finished: {produced:,} records published out of {self.total_available:,} available")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue with batch inserts and persistent connection"""
        pg_conn = None
        try:
            self.logger.info("Consumer thread started")
            
            # Setup persistent PostgreSQL connection
            pg_conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            self.logger.info("Consumer: Persistent PostgreSQL connection established")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Batch insert buffer
            insert_buffer: List[AccountProductCategoryRecord] = []
            pending_tags: List[int] = []
            last_flush_time = time.time()
            flush_interval = 5  # seconds
            last_progress_report = time.time()
            
            def flush_buffer(ch):
                """Flush buffered records to PostgreSQL in a single batch insert"""
                nonlocal insert_buffer, pending_tags, last_flush_time, pg_conn
                if not insert_buffer:
                    return
                
                batch_size = len(insert_buffer)
                try:
                    self.insert_batch_to_postgres(insert_buffer, pg_conn)
                    
                    # Batch acknowledge all messages at once
                    if pending_tags:
                        ch.basic_ack(delivery_tag=pending_tags[-1], multiple=True)
                    
                    with self._stats_lock:
                        self.total_consumed += batch_size
                    
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
                    
                except psycopg2.OperationalError as e:
                    self.logger.error(f"PostgreSQL connection lost during batch insert: {e}")
                    # Reconnect PostgreSQL
                    try:
                        pg_conn.close()
                    except Exception:
                        pass
                    pg_conn = psycopg2.connect(
                        host=self.config.database.pg_host,
                        port=self.config.database.pg_port,
                        database=self.config.database.pg_database,
                        user=self.config.database.pg_user,
                        password=self.config.database.pg_password
                    )
                    # Nack all to requeue for retry
                    for tag in pending_tags:
                        try:
                            ch.basic_nack(delivery_tag=tag, requeue=True)
                        except Exception:
                            pass
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
                    
                except Exception as e:
                    self.logger.error(f"Batch insert failed: {e}")
                    try:
                        pg_conn.rollback()
                    except Exception:
                        pass
                    # Nack all messages - they go to dead-letter queue
                    for tag in pending_tags:
                        try:
                            ch.basic_nack(delivery_tag=tag, requeue=False)
                        except Exception:
                            pass
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal insert_buffer, pending_tags, last_progress_report, last_flush_time
                try:
                    record_data = json.loads(body)
                    record = AccountProductCategoryRecord(**record_data)
                    
                    insert_buffer.append(record)
                    pending_tags.append(method.delivery_tag)
                    
                    # Flush if buffer is full or time interval exceeded
                    if len(insert_buffer) >= self.consumer_batch_size or \
                       time.time() - last_flush_time >= flush_interval:
                        flush_buffer(ch)
                    
                    # Progress monitoring
                    with self._stats_lock:
                        consumed = self.total_consumed
                    
                    if consumed > 0 and consumed % self.batch_size == 0:
                        elapsed_time = time.time() - self.start_time
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        progress_percent = (consumed / self.total_available * 100) if self.total_available > 0 else 0
                        
                        self.logger.info(f"Consumer: Processed {consumed:,} records ({progress_percent:.2f}% of total) - Rate: {rate:.1f} rec/sec")
                    
                    # Detailed progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        with self._stats_lock:
                            consumed = self.total_consumed
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - consumed if self.total_available > 0 else 0
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"CONSUMER PROGRESS: {consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS to match consumer batch size for efficient batching
            channel.basic_qos(prefetch_count=self.consumer_batch_size)
            channel.basic_consume(queue='account_product_category_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            empty_checks = 0
            max_empty_checks = 3  # Check 3 times before stopping
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Flush any remaining buffered records on timeout
                    if insert_buffer and time.time() - last_flush_time >= flush_interval:
                        flush_buffer(channel)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Flush remaining buffer before checking queue
                        flush_buffer(channel)
                        
                        # Producer is done, check if queue is empty
                        queue_state = channel.queue_declare(queue='account_product_category_queue', durable=True, passive=True)
                        message_count = queue_state.method.message_count
                        
                        if message_count == 0:
                            empty_checks += 1
                            self.logger.info(f"Consumer: Queue appears empty (check {empty_checks}/{max_empty_checks})")
                            
                            if empty_checks >= max_empty_checks:
                                self.logger.info("Consumer: Queue confirmed empty after multiple checks, producer finished")
                                break
                            else:
                                # Wait a bit before checking again
                                time.sleep(2)
                        else:
                            # Reset counter if we find messages
                            empty_checks = 0
                            self.logger.info(f"Consumer: {message_count} messages still in queue, continuing...")
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect RabbitMQ
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='account_product_category_queue', on_message_callback=process_message)
            
            connection.close()
            with self._stats_lock:
                consumed = self.total_consumed
            self.logger.info(f"Consumer finished: {consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
        finally:
            if pg_conn:
                try:
                    pg_conn.close()
                except Exception:
                    pass
    
    def insert_batch_to_postgres(self, records: List[AccountProductCategoryRecord], pg_conn):
        """Batch insert account product category records to PostgreSQL with duplicate prevention"""
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "accountProductCategory" (
                "reportingDate", "accountProductCode", "accountProductName", "accountProductDescription",
                "accountProductType", "accountProductSubType", currency, "accountProductCreationDate",
                "accountProductClosureDate", "accountProductStatus"
            ) VALUES %s
            ON CONFLICT ("accountProductCode") DO NOTHING
            """
            
            values = [
                (
                    r.reportingDate, r.accountProductCode, r.accountProductName, r.accountProductDescription,
                    r.accountProductType, r.accountProductSubType, r.currency, r.accountProductCreationDate,
                    r.accountProductClosureDate, r.accountProductStatus
                )
                for r in records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} account product category records: {e}")
            raise
    
    def ensure_unique_index(self):
        """Ensure unique index on accountProductCode exists for ON CONFLICT duplicate prevention"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_apc_code_unique
                    ON "accountProductCategory"("accountProductCode")
                """)
                conn.commit()
                self.logger.info("Unique index on accountProductCode verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on accountProductCategory: {e}")
            raise
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Account Product Category STREAMING pipeline...")
        
        try:
            # Ensure unique index for duplicate prevention
            self.ensure_unique_index()
            
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
            consumer_thread.join(timeout=60)
            
            if consumer_thread.is_alive():
                self.logger.info("Stopping consumer thread...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)
            
            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            
            self.logger.info("=" * 60)
            self.logger.info("Account Product Category STREAMING Pipeline Complete!")
            self.logger.info("=" * 60)
            self.logger.info(f"Total records processed: {self.total_consumed:,}")
            self.logger.info(f"Total time: {total_time:.1f} seconds")
            self.logger.info(f"Average rate: {avg_rate:.1f} records/second")
            self.logger.info("=" * 60)
            
            return {
                'total_produced': self.total_produced,
                'total_consumed': self.total_consumed,
                'total_time': total_time,
                'avg_rate': avg_rate
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


if __name__ == "__main__":
    pipeline = AccountProductCategoryStreamingPipeline()
    pipeline.run_streaming_pipeline()