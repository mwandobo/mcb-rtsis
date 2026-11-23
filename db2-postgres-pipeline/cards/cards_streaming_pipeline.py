#!/usr/bin/env python3
"""
Cards Streaming Pipeline - Producer and Consumer run simultaneously
Based on card_information.sql query
"""

import pika
import psycopg2
import psycopg2.extras
import json
import logging
import re
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
class CardRecord:
    """Data class for card records based on card_information.sql"""
    reportingDate: str
    bankCode: str
    cardNumber: str
    binNumber: str
    customerIdentificationNumber: str
    cardType: str
    cardTypeSubCategory: Optional[str]
    cardIssueDate: str
    cardIssuer: str
    cardIssuerCategory: str
    cardIssuerCountry: str
    cardHolderName: str
    cardStatus: str
    cardScheme: str
    acquiringPartner: str
    cardExpireDate: str


class CardsStreamingPipeline:
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
        
        self.logger.info("Cards STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_cards_query(self, last_timestamp=None):
        """Get the cards query from card_information.sql, with optional TMSTAMP filter for incremental mode"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'card_information.sql'
        )

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read().strip()

        if sql.endswith(';'):
            sql = sql[:-1]

        # For incremental mode, add a TMSTAMP filter on CMS_CARD. The query
        # has no existing WHERE clause, so this is a plain append (no need
        # to parenthesize anything, unlike pipelines whose base query already
        # has a WHERE predicate).
        if last_timestamp:
            if last_timestamp == 'epoch':
                timestamp_value = '1900-01-01-00.00.00.000000'
            else:
                timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]

            tmstamp_filter = f"CA.TMSTAMP > '{timestamp_value}'"
            order_by_match = re.search(r'\bORDER\s+BY\b', sql, re.IGNORECASE)
            tail_start = order_by_match.start() if order_by_match else len(sql)
            tail = sql[tail_start:]

            where_match = re.search(r'\bWHERE\b', sql, re.IGNORECASE)
            if where_match:
                head = sql[:where_match.start()]
                existing_predicate = sql[where_match.end():tail_start].strip()
                sql = f"{head}WHERE ({existing_predicate}) AND {tmstamp_filter}\n{tail}"
            else:
                head = sql[:tail_start]
                sql = f"{head}\nWHERE {tmstamp_filter}\n{tail}"

        return sql

    def get_last_successful_run(self):
        """Get last successful run timestamp from state manager"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            return state_manager.get_last_successful_run('cards')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None

    def get_total_count(self):
        """Get approximate total count of card records from DB2"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM CMS_CARD")
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Estimated record count from CMS_CARD: {count:,}")
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
        """Setup RabbitMQ queue for cards with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='cards_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='cards_dead_letter', durable=True)
            channel.queue_bind(
                queue='cards_dead_letter',
                exchange='cards_dlx',
                routing_key='cards_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='cards_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'cards_dlx',
                        'x-dead-letter-routing-key': 'cards_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments
                self.logger.warning(
                    "Queue 'cards_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='cards_queue', durable=True)
                self.logger.info("RabbitMQ queue 'cards_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single card record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            # Map the fields from the SQL query to the dataclass
            record = CardRecord(
                reportingDate=safe_string(row[0]),                      # VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYYHHMM')
                bankCode=safe_string(row[1]),                           # 'MWCOTZTZ'
                cardNumber=safe_string(row[2]),                         # CA.FULL_CARD_NO
                binNumber=safe_string(row[3]),                          # RIGHT(TRIM(CA.FULL_CARD_NO), 10)
                customerIdentificationNumber=safe_string(row[4]),       # CA.FK_CUST_ID
                cardType=safe_string(row[5]),                           # 'Debit'
                cardTypeSubCategory=safe_string(row[6]) if row[6] else None,  # NULL
                cardIssueDate=safe_string(row[7]),                      # VARCHAR_FORMAT(CA.TUN_DATE,'DDMMYYYYHHMM')
                cardIssuer=safe_string(row[8]),                         # 'Mwalimu Commercial Bank Plc'
                cardIssuerCategory=safe_string(row[9]),                 # 'Domestic'
                cardIssuerCountry=safe_string(row[10]),                 # 'TANZANIA, UNITED REPUBLIC OF'
                cardHolderName=safe_string(row[11]),                    # CA.CARD_NAME_LATIN
                cardStatus=safe_string(row[12]),                        # CASE WHEN CURRENT_DATE > CA.CARD_EXPIRY_DATE...
                cardScheme=safe_string(row[13]),                        # 'VISA'
                acquiringPartner=safe_string(row[14]),                  # 'UBX Tanzania Limited'
                cardExpireDate=safe_string(row[15])                     # VARCHAR_FORMAT(CA.CARD_EXPIRY_DATE,'DDMMYYYYHHMM')
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing card record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate card record"""
        try:
            # Basic validation
            if not record.cardNumber:
                self.logger.warning("Missing card number")
                return False
            
            if not record.customerIdentificationNumber:
                self.logger.warning("Missing customer identification number")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating card record: {e}")
            return False
    
    def producer_thread(self, incremental=True):
        """Producer thread - executes query ONCE and streams results via fetchmany()"""
        try:
            self.logger.info("Producer thread started")

            last_timestamp = None
            if incremental:
                last_timestamp = self.get_last_successful_run()
                if last_timestamp:
                    self.logger.info(f"Incremental mode: fetching records with TMSTAMP > {last_timestamp}")
                else:
                    last_timestamp = 'epoch'
                    self.logger.info("Incremental mode: first run, fetching all records (epoch)")
            else:
                self.logger.info("Full load mode: fetching all records")

            # Get dynamic record count
            self.total_available = self.get_total_count()

            self.logger.info(f"Total card records available: {self.total_available:,} (estimated)")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")

            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()

            # Execute the query ONCE and stream results
            query = self.get_cards_query(last_timestamp)
            self.logger.info("Executing cards query (single execution, streaming results)...")
            
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
                                        routing_key='cards_queue',
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
            insert_buffer: List[CardRecord] = []
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
                    record = CardRecord(**record_data)
                    
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
            channel.basic_consume(queue='cards_queue', on_message_callback=process_message)

            # Keep consuming until producer is done and queue is empty
            empty_checks = 0
            max_empty_checks = 3  # require 3 consecutive empty reads before stopping
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

                        # Producer is done, check if queue is empty. A single
                        # passive queue_declare can race against messages that
                        # were published just before the producer's connection
                        # closed but haven't yet been delivered to this
                        # consumer - a single "empty" reading isn't reliable
                        # proof nothing is left, so require 3 consecutive
                        # empty reads (2s apart) before concluding we're done.
                        queue_state = channel.queue_declare(queue='cards_queue', durable=True, passive=True)
                        if queue_state.method.message_count == 0:
                            empty_checks += 1
                            self.logger.info(f"Consumer: Queue appears empty (check {empty_checks}/{max_empty_checks})")
                            if empty_checks >= max_empty_checks:
                                self.logger.info("Consumer: Queue confirmed empty after multiple checks, producer finished")
                                break
                            else:
                                time.sleep(2)
                        else:
                            empty_checks = 0
                            self.logger.info(f"Consumer: {queue_state.method.message_count} messages still in queue, continuing...")

                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect RabbitMQ
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='cards_queue', on_message_callback=process_message)
            
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
    
    def insert_batch_to_postgres(self, records: List[CardRecord], pg_conn):
        """Batch insert card records to PostgreSQL with duplicate prevention"""
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "cardInformation" (
                "reportingDate", "bankCode", "cardNumber", "binNumber",
                "customerIdentificationNumber", "cardType", "cardTypeSubCategory",
                "cardIssueDate", "cardIssuer", "cardIssuerCategory", "cardIssuerCountry",
                "cardHolderName", "cardStatus", "cardScheme", "acquiringPartner",
                "cardExpireDate"
            ) VALUES %s
            ON CONFLICT ("cardNumber") DO UPDATE SET
                "reportingDate" = EXCLUDED."reportingDate",
                "bankCode" = EXCLUDED."bankCode",
                "binNumber" = EXCLUDED."binNumber",
                "customerIdentificationNumber" = EXCLUDED."customerIdentificationNumber",
                "cardType" = EXCLUDED."cardType",
                "cardTypeSubCategory" = EXCLUDED."cardTypeSubCategory",
                "cardIssueDate" = EXCLUDED."cardIssueDate",
                "cardIssuer" = EXCLUDED."cardIssuer",
                "cardIssuerCategory" = EXCLUDED."cardIssuerCategory",
                "cardIssuerCountry" = EXCLUDED."cardIssuerCountry",
                "cardHolderName" = EXCLUDED."cardHolderName",
                "cardStatus" = EXCLUDED."cardStatus",
                "cardScheme" = EXCLUDED."cardScheme",
                "acquiringPartner" = EXCLUDED."acquiringPartner",
                "cardExpireDate" = EXCLUDED."cardExpireDate",
                updated_at = CURRENT_TIMESTAMP
            """
            
            values = [
                (
                    r.reportingDate, r.bankCode, r.cardNumber, r.binNumber,
                    r.customerIdentificationNumber, r.cardType, r.cardTypeSubCategory,
                    r.cardIssueDate, r.cardIssuer, r.cardIssuerCategory, r.cardIssuerCountry,
                    r.cardHolderName, r.cardStatus, r.cardScheme, r.acquiringPartner,
                    r.cardExpireDate
                )
                for r in records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} card records: {e}")
            raise
    
    def setup_target_table(self):
        """Create the cardInformation table if it doesn't exist yet (idempotent, non-destructive)"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS "cardInformation" (
                        "id" SERIAL PRIMARY KEY,
                        "reportingDate" VARCHAR(50),
                        "bankCode" VARCHAR(50),
                        "cardNumber" VARCHAR(100) NOT NULL,
                        "binNumber" VARCHAR(50),
                        "customerIdentificationNumber" VARCHAR(100),
                        "cardType" VARCHAR(50),
                        "cardTypeSubCategory" VARCHAR(100),
                        "cardIssueDate" VARCHAR(50),
                        "cardIssuer" VARCHAR(200),
                        "cardIssuerCategory" VARCHAR(100),
                        "cardIssuerCountry" VARCHAR(100),
                        "cardHolderName" VARCHAR(200),
                        "cardStatus" VARCHAR(50),
                        "cardScheme" VARCHAR(100),
                        "acquiringPartner" VARCHAR(200),
                        "cardExpireDate" VARCHAR(50),
                        "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute('ALTER TABLE "cardInformation" ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
                conn.commit()
                self.logger.info("cardInformation table verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create cardInformation table: {e}")
            raise

    def ensure_unique_index(self):
        """Ensure unique index on cardNumber exists for ON CONFLICT duplicate prevention"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_cardinformation_cardnumber_unique
                    ON "cardInformation" ("cardNumber")
                """)
                conn.commit()
                self.logger.info("Unique index on cardNumber verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on cardNumber: {e}")
            raise
    
    def run_streaming_pipeline(self, incremental=True):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Cards STREAMING pipeline...")
        self._update_state('running')

        try:
            # Ensure target table and unique index exist
            self.setup_target_table()
            self.ensure_unique_index()

            # Setup queue
            self.setup_rabbitmq_queue()

            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()

            # Small delay to let consumer start
            time.sleep(1)

            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer", args=(incremental,))
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
            Cards Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/3600:.2f} hours
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)

            self._update_state('completed')

        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            self._update_state('failed', str(e))
            raise

    def _update_state(self, status, error_message=None):
        try:
            from pipeline_state import PipelineStateManager
            PipelineStateManager().update_run('cards', status, self.total_consumed, error_message)
            self.logger.info(f"State updated: {status}, records: {self.total_consumed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Cards Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for DB2 query pagination')
    parser.add_argument('--consumer-batch-size', type=int, default=100, help='Batch size for PostgreSQL inserts')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')

    args = parser.parse_args()

    # Create pipeline
    pipeline = CardsStreamingPipeline(batch_size=args.batch_size, consumer_batch_size=args.consumer_batch_size)

    try:
        if args.mode == 'producer':
            pipeline.producer_thread(incremental=not args.full_load)
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:  # streaming
            pipeline.run_streaming_pipeline(incremental=not args.full_load)

    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
