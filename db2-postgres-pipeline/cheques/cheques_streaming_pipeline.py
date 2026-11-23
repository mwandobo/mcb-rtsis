#!/usr/bin/env python3
"""
Cheques Streaming Pipeline - Producer and Consumer run simultaneously
Uses cheque-and-other-items-for-clearing.sql query for comprehensive cheque data extraction
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

# Configure logging at module level (should only be called once)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class ChequeRecord:
    """Data class for cheque records based on cheque-and-other-items-for-clearing.sql"""
    reportingDate: str
    chequeNumber: str
    issuerName: str
    issuerBankerCode: str
    payeeName: str
    payeeAccountNumber: str
    chequeDate: str
    transactionDate: str
    settlementDate: str
    allowanceProbableLoss: str
    botProvision: str
    currency: str
    orgAmountOpening: str
    usdAmountOpening: str
    tzsAmountOpening: str
    orgAmountPayment: str
    usdAmountPayment: str
    tzsAmountPayment: str
    orgAmountBalance: str
    usdAmountBalance: str
    tzsAmountBalance: str
    tmstamp: str
    billSerialNum: Optional[str]


class ChequesStreamingPipeline:
    def __init__(self, batch_size=1000):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = 100  # Fixed value
        
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
        
        self.logger.info("Cheques STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_cheques_query(self, last_timestamp=None):
        """Get the cheques query using cheque-and-other-items-for-clearing.sql with optional TMSTAMP filter"""
        
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'cheque-and-other-items-for-clearing-v1.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # For incremental mode, add TMSTAMP filter on CHEQUES_FOR_COLLEC table
        if last_timestamp:
            if last_timestamp == 'epoch':
                timestamp_value = '1900-01-01-00.00.00.000000'
            else:
                timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]
            
            # Add the timestamp filter to the WHERE clause
            # Look for the existing WHERE clause and add our condition
            if 'where bic.BIC is not null' in sql.lower():
                sql = sql.replace(
                    'where bic.BIC is not null',
                    f"where bic.BIC is not null AND cfc.TMSTAMP > '{timestamp_value}'"
                )
            else:
                # If no WHERE clause exists, add one
                sql += f" WHERE cfc.TMSTAMP > '{timestamp_value}'"
        
        # Add ORDER BY TMSTAMP for incremental processing
        if 'ORDER BY' not in sql.upper():
            sql += " ORDER BY cfc.TMSTAMP"
        
        return sql
    
    def get_last_successful_run(self):
        """Get last successful run timestamp from state manager"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            return state_manager.get_last_successful_run('cheques')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None
    
    def get_total_count(self, last_timestamp=None):
        """Get fast approximate count from base table.
        Complex JOIN count queries are too slow - use simple base table count.
        """
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                
                # Simple fast count from base table
                if last_timestamp:
                    if last_timestamp == 'epoch':
                        timestamp_value = '1900-01-01-00.00.00.000000'
                    else:
                        timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]
                    cursor.execute(f"SELECT COUNT(*) FROM CHEQUES_FOR_COLLEC WHERE TMSTAMP > '{timestamp_value}'")
                else:
                    cursor.execute("SELECT COUNT(*) FROM CHEQUES_FOR_COLLEC")
                
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Estimated record count from CHEQUES_FOR_COLLEC: {count:,}")
                return count
        except Exception as e:
            self.logger.warning(f"Could not fetch record count, progress %% unavailable: {e}")
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
        """Setup RabbitMQ queue for cheques with dead-letter exchange for failed messages"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='cheques_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='cheques_dead_letter', durable=True)
            channel.queue_bind(
                queue='cheques_dead_letter',
                exchange='cheques_dlx',
                routing_key='cheques_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='cheques_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'cheques_dlx',
                        'x-dead-letter-routing-key': 'cheques_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments; reconnect and use as-is
                self.logger.warning(
                    "Queue 'cheques_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='cheques_queue', durable=True)
                self.logger.info("RabbitMQ queue 'cheques_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single cheque record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            # Map the fields from the SQL query to the dataclass
            # Based on the exact field order in cheque-and-other-items-for-clearing.sql:
            record = ChequeRecord(
                reportingDate=safe_string(row[0]),                      # VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')
                chequeNumber=safe_string(row[1]),                       # cfc.CHEQUE_NUMBER
                issuerName=safe_string(row[2]),                         # RTRIM(LTRIM(COALESCE(ic.FIRST_NAME, '') || ' ' || ...))
                issuerBankerCode=safe_string(row[3]),                   # bic.BIC
                payeeName=safe_string(row[4]),                          # RTRIM(LTRIM(COALESCE(pc.FIRST_NAME, '') || ' ' || ...))
                payeeAccountNumber=safe_string(row[5]),                 # ppa.ACCOUNT_NUMBER
                chequeDate=safe_string(row[6]),                         # VARCHAR_FORMAT(cfc.ISSUE_DATE, 'DDMMYYYYHHMM')
                transactionDate=safe_string(row[7]),                    # VARCHAR_FORMAT(cfc.TRX_DATE, 'DDMMYYYYHHMM')
                settlementDate=safe_string(row[8]),                     # VARCHAR_FORMAT(cfc.BEAR_PAYMENT_DATE, 'DDMMYYYYHHMM')
                allowanceProbableLoss=safe_string(row[9]),              # 0
                botProvision=safe_string(row[10]),                      # 0
                currency=safe_string(row[11]),                          # cu.SHORT_DESCR
                orgAmountOpening=safe_string(row[12]),                  # CAST(da.OPENING_BALANCE AS DECFLOAT)
                usdAmountOpening=safe_string(row[13]),                  # 0
                tzsAmountOpening=safe_string(row[14]),                  # CAST(da.OPENING_BALANCE AS DECFLOAT)
                orgAmountPayment=safe_string(row[15]),                  # CAST(cfc.CHEQUE_AMOUNT AS DECFLOAT)
                usdAmountPayment=safe_string(row[16]),                  # 0
                tzsAmountPayment=safe_string(row[17]),                  # CAST(cfc.CHEQUE_AMOUNT AS DECFLOAT)
                orgAmountBalance=safe_string(row[18]),                  # CAST(da.BOOK_BALANCE AS DECFLOAT)
                usdAmountBalance=safe_string(row[19]),                  # 0
                tzsAmountBalance=safe_string(row[20]),                  # CAST(da.BOOK_BALANCE AS DECFLOAT)
                tmstamp=safe_string(row[21]),                           # cfc.TMSTAMP
                billSerialNum=safe_string(row[22]) if row[22] else None  # cfc.BILL_SERIAL_NUM
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing cheque record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate cheque record"""
        try:
            # Basic validation
            if not record.chequeNumber:
                self.logger.warning("Missing cheque number")
                return False
            
            if not record.issuerName:
                self.logger.warning("Missing issuer name")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating cheque record: {e}")
            return False
    
    def producer_thread(self, incremental=True):
        """Producer thread - executes cheques query ONCE and streams results via fetchmany().
        
        Instead of re-executing the expensive query for every batch,
        we run it once and use the DB2 server-side cursor to fetch rows
        in chunks.
        """
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
            
            # Get dynamic record count with same filters as main query
            self.total_available = self.get_total_count(last_timestamp)
            
            self.logger.info(f"Total cheque records available: {self.total_available:,} (estimated)")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()
            
            # Execute the cheques query ONCE and stream results
            query = self.get_cheques_query(last_timestamp)
            self.logger.info("Executing cheques query (single execution, streaming results)...")
            
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
                                        routing_key='cheques_queue',
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
                    
                    # Calculate progress percentage with dynamic adjustment
                    if self.total_available > 0:
                        if produced <= self.total_available:
                            progress_percent = produced / self.total_available * 100
                        else:
                            # We've exceeded initial estimate - recalculate based on actual progress
                            progress_percent = 100.0  # Cap at 100% for display
                            # Update total available to reflect actual data size
                            self.total_available = max(self.total_available, produced)
                    else:
                        progress_percent = 0
                    
                    self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} rows fetched, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                    
                    # Progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        rate = produced / elapsed_time if elapsed_time > 0 else 0
                        
                        # Calculate progress with dynamic total
                        if produced <= self.total_available:
                            progress_percent = produced / self.total_available * 100 if self.total_available > 0 else 0
                            remaining_records = self.total_available - produced
                        else:
                            progress_percent = 100.0
                            remaining_records = 0  # Unknown remaining
                            
                        eta_seconds = remaining_records / rate if rate > 0 and remaining_records > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        if remaining_records > 0:
                            self.logger.info(f"PROGRESS REPORT: {produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        else:
                            self.logger.info(f"PROGRESS REPORT: {produced:,} records processed (exceeded initial estimate) - Rate: {rate:.1f} rec/sec")
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
            
            # Setup persistent PostgreSQL connection (reused across all messages)
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
            insert_buffer: List[ChequeRecord] = []
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
                    record = ChequeRecord(**record_data)
                    
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
            channel.basic_consume(queue='cheques_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Flush any remaining buffered records periodically
                    if insert_buffer and time.time() - last_flush_time >= flush_interval:
                        flush_buffer(channel)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Flush remaining buffer before checking queue
                        flush_buffer(channel)
                        
                        # Producer is done, check if queue is empty
                        queue_state = channel.queue_declare(queue='cheques_queue', durable=True, passive=True)
                        if queue_state.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect RabbitMQ
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='cheques_queue', on_message_callback=process_message)
            
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
    
    def insert_batch_to_postgres(self, records: List[ChequeRecord], pg_conn):
        """Batch insert cheque records to PostgreSQL with duplicate prevention.
        
        Uses psycopg2.extras.execute_values for efficient batch inserts and
        ON CONFLICT to skip duplicate chequeNumbers.
        """
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "chequeClearing" (
                "reportingDate", "chequeNumber", "issuerName", "issuerBankerCode", "payeeName",
                "payeeAccountNumber", "chequeDate", "transactionDate", "settlementDate",
                "allowanceProbableLoss", "botProvision", "currency", "orgAmountOpening",
                "usdAmountOpening", "tzsAmountOpening", "orgAmountPayment", "usdAmountPayment",
                "tzsAmountPayment", "orgAmountBalance", "usdAmountBalance", "tzsAmountBalance",
                "billSerialNum"
            ) VALUES %s
            ON CONFLICT ("billSerialNum") DO UPDATE SET
                "reportingDate" = EXCLUDED."reportingDate",
                "chequeNumber" = EXCLUDED."chequeNumber",
                "issuerName" = EXCLUDED."issuerName",
                "issuerBankerCode" = EXCLUDED."issuerBankerCode",
                "payeeName" = EXCLUDED."payeeName",
                "payeeAccountNumber" = EXCLUDED."payeeAccountNumber",
                "chequeDate" = EXCLUDED."chequeDate",
                "transactionDate" = EXCLUDED."transactionDate",
                "settlementDate" = EXCLUDED."settlementDate",
                "allowanceProbableLoss" = EXCLUDED."allowanceProbableLoss",
                "botProvision" = EXCLUDED."botProvision",
                currency = EXCLUDED.currency,
                "orgAmountOpening" = EXCLUDED."orgAmountOpening",
                "usdAmountOpening" = EXCLUDED."usdAmountOpening",
                "tzsAmountOpening" = EXCLUDED."tzsAmountOpening",
                "orgAmountPayment" = EXCLUDED."orgAmountPayment",
                "usdAmountPayment" = EXCLUDED."usdAmountPayment",
                "tzsAmountPayment" = EXCLUDED."tzsAmountPayment",
                "orgAmountBalance" = EXCLUDED."orgAmountBalance",
                "usdAmountBalance" = EXCLUDED."usdAmountBalance",
                "tzsAmountBalance" = EXCLUDED."tzsAmountBalance",
                updated_at = CURRENT_TIMESTAMP
            """

            values = [
                (
                    r.reportingDate, r.chequeNumber, r.issuerName, r.issuerBankerCode, r.payeeName,
                    r.payeeAccountNumber, r.chequeDate, r.transactionDate, r.settlementDate,
                    r.allowanceProbableLoss, r.botProvision, r.currency, r.orgAmountOpening,
                    r.usdAmountOpening, r.tzsAmountOpening, r.orgAmountPayment, r.usdAmountPayment,
                    r.tzsAmountPayment, r.orgAmountBalance, r.usdAmountBalance, r.tzsAmountBalance,
                    r.billSerialNum
                )
                for r in records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} cheque records: {e}")
            raise
    
    def setup_target_table(self):
        """Create the chequeClearing table and add the dedup-key column if missing (idempotent, non-destructive)"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS "chequeClearing" (
                        "id" SERIAL PRIMARY KEY,
                        "reportingDate" VARCHAR(50),
                        "chequeNumber" VARCHAR(100),
                        "issuerName" VARCHAR(255),
                        "issuerBankerCode" VARCHAR(50),
                        "payeeName" VARCHAR(255),
                        "payeeAccountNumber" VARCHAR(100),
                        "chequeDate" VARCHAR(50),
                        "transactionDate" VARCHAR(50),
                        "settlementDate" VARCHAR(50),
                        "allowanceProbableLoss" VARCHAR(50),
                        "botProvision" VARCHAR(50),
                        "currency" VARCHAR(10),
                        "orgAmountOpening" VARCHAR(50),
                        "usdAmountOpening" VARCHAR(50),
                        "tzsAmountOpening" VARCHAR(50),
                        "orgAmountPayment" VARCHAR(50),
                        "usdAmountPayment" VARCHAR(50),
                        "tzsAmountPayment" VARCHAR(50),
                        "orgAmountBalance" VARCHAR(50),
                        "usdAmountBalance" VARCHAR(50),
                        "tzsAmountBalance" VARCHAR(50),
                        "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute('ALTER TABLE "chequeClearing" ADD COLUMN IF NOT EXISTS "billSerialNum" VARCHAR(50)')
                cursor.execute('ALTER TABLE "chequeClearing" ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
                conn.commit()
                self.logger.info("chequeClearing table and dedup-key column verified/created")
        except Exception as e:
            self.logger.error(f"Failed to set up chequeClearing table: {e}")
            raise

    def ensure_unique_index(self):
        """Ensure performance indexes and the dedup unique index exist"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_chequeclearing_number
                    ON "chequeClearing" ("chequeNumber")
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_chequeclearing_date
                    ON "chequeClearing" ("transactionDate")
                """)
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_chequeclearing_bill_serial_num
                    ON "chequeClearing" ("billSerialNum")
                """)
                conn.commit()
                self.logger.info("Performance and unique indexes on chequeClearing verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create indexes: {e}")
            raise

    def run_streaming_pipeline(self, incremental=True):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info(f"Starting Cheques STREAMING pipeline... (incremental={incremental})")

        try:
            # Update state to running
            self._update_state('running')

            # Ensure target table and indexes exist
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
            Cheques Pipeline Summary:
            ==========================================
            Mode: {'Incremental' if incremental else 'Full Load'}
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/3600:.2f} hours
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
            # Update pipeline state
            self._update_state('completed')
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            self._update_state('failed', str(e))
            raise
    
    def _update_state(self, status, error_message=None):
        """Update pipeline state in the state table"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            state_manager.update_run(
                'cheques',
                status,
                self.total_consumed,
                error_message
            )
            self.logger.info(f"State updated: {status}, records: {self.total_consumed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Cheques Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = ChequesStreamingPipeline(batch_size=args.batch_size)
    
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