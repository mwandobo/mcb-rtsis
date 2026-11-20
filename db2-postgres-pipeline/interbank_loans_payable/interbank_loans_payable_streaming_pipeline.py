#!/usr/bin/env python3
"""
Interbank Loans Payable Streaming Pipeline - Producer and Consumer run simultaneously
Based on interbank_loans_payable.sql query
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
from pipeline_state import PipelineStateManager

# Configure logging at module level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class InterbankLoansPayableRecord:
    """Data class for interbank loans payable records based on interbank_loans_payable.sql"""
    reportingDate: str
    lenderName: str
    accountNumber: str
    lenderCountry: str
    borrowingType: str
    transactionDate: str
    disbursementDate: str
    maturityDate: str
    currency: str
    orgAmountOpening: str
    usdAmountOpening: str
    tzsAmountOpening: str
    orgAmountRepayment: str
    usdAmountRepayment: str
    tzsAmountRepayment: str
    orgAmountClosing: str
    usdAmountClosing: str
    tzsAmountClosing: str
    tenureDays: Optional[str]
    annualInterestRate: str
    interestRateType: str
    dealNumber: str


class InterbankLoansPayableStreamingPipeline:
    def __init__(self, batch_size=1000):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = 100  # Fixed value per pipeline standards
        
        # State management for incremental processing
        self.state_manager = PipelineStateManager()
        
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
        
        self.logger.info("Interbank Loans Payable STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_last_successful_run(self):
        """Get the timestamp of the last successful pipeline run"""
        return self.state_manager.get_last_successful_run('interbank_loans_payable')
    
    def _update_state(self, timestamp):
        """Record the completed incremental run in the shared pipeline state table."""
        self.state_manager.update_run(
            'interbank_loans_payable',
            'completed',
            self.total_consumed,
        )
    
    def get_interbank_loans_payable_query(self, last_timestamp=None):
        """Get the interbank loans payable query from interbank_loans_payable.sql with optional timestamp filter"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'interbank_loans_payable.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            query = f.read()
        
        # Add incremental filter if timestamp provided
        if last_timestamp:
            # Find the WHERE clause and add timestamp filter
            query_lines = query.strip().split('\n')
            
            # Find the WHERE clause line
            where_line_idx = -1
            for i, line in enumerate(query_lines):
                if line.strip().upper().startswith('WHERE '):
                    where_line_idx = i
                    break
            
            if where_line_idx >= 0:
                # Add timestamp filter to existing WHERE clause
                query_lines.insert(where_line_idx + 1, f"  AND mm.TMSTAMP > '{last_timestamp}'")
            else:
                # No WHERE clause found, add one before ORDER BY
                order_by_idx = -1
                for i, line in enumerate(query_lines):
                    if line.strip().upper().startswith('ORDER BY'):
                        order_by_idx = i
                        break
                
                if order_by_idx >= 0:
                    query_lines.insert(order_by_idx, f"WHERE mm.TMSTAMP > '{last_timestamp}'")
                    query_lines.insert(order_by_idx + 1, "")  # Add blank line
                else:
                    # No ORDER BY found, add WHERE at the end
                    query_lines.append(f"WHERE mm.TMSTAMP > '{last_timestamp}'")
            
            query = '\n'.join(query_lines)
        
        return query
    
    def get_total_count(self, last_timestamp=None):
        """Get accurate total count of interbank loans payable records from DB2 using the same query structure"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                
                # Build count query with same filters as main query
                count_query = """
                SELECT COUNT(*) FROM (
                    SELECT mm.DEAL_NO
                    FROM TREASURY_MM_DEAL mm
                    
                    -- Same joins as main query
                    JOIN COLLABORATION_BANK cb
                        ON mm.FK_DEAL_COL_BANK = cb.BANK_ID
                    
                    JOIN CURRENCY cur
                        ON mm.FK_SOURCE_CURRENCY = cur.ID_CURRENCY
                    
                    LEFT JOIN COUNTRIES_LOOKUP cl
                        ON cb.CNTRY_ISO_CODE = cl.COUNTRY_CODE

                    LEFT JOIN (
                        SELECT
                            gli.DEAL_NO,
                            SUM(CASE WHEN gli.ENTRY_TYPE = 'D' THEN gli.AMOUNT ELSE 0 END) AS org_repayment
                        FROM FXFT_GLI_INTERFACE gli
                        WHERE gli.FK_GLG_ACCOUNTACCO = '1.0.2.00.0001'
                          AND gli.DEAL_NO IS NOT NULL
                        GROUP BY gli.DEAL_NO
                    ) repay ON mm.DEAL_NO = repay.DEAL_NO
                    
                    LEFT JOIN CURRENCY cur_usd ON cur_usd.SHORT_DESCR = 'USD'
                    
                    LEFT JOIN FIXING_RATE fr_src
                        ON fr_src.FK_CURRENCYID_CURR = mm.FK_SOURCE_CURRENCY
                        AND fr_src.ACTIVATION_DATE = (
                            SELECT MAX(f1.ACTIVATION_DATE)
                            FROM FIXING_RATE f1
                            WHERE f1.FK_CURRENCYID_CURR = mm.FK_SOURCE_CURRENCY
                              AND f1.ACTIVATION_DATE <= CURRENT DATE
                        )
                    
                    LEFT JOIN FIXING_RATE fr_usd
                        ON fr_usd.FK_CURRENCYID_CURR = cur_usd.ID_CURRENCY
                        AND fr_usd.ACTIVATION_DATE = (
                            SELECT MAX(f2.ACTIVATION_DATE)
                            FROM FIXING_RATE f2
                            WHERE f2.FK_CURRENCYID_CURR = cur_usd.ID_CURRENCY
                              AND f2.ACTIVATION_DATE <= CURRENT DATE
                        )
                    
                    -- Same filters as main query
                    WHERE mm.DEAL_OPERATION = 'B'
                      AND mm.STATUS != 'C'
                """
                
                # Add incremental filter if timestamp provided
                if last_timestamp:
                    count_query += f"      AND mm.TMSTAMP > '{last_timestamp}'"
                
                count_query += """
                ) AS count_subquery
                """
                
                cursor.execute(count_query)
                result = cursor.fetchone()
                count = result[0] if result else 0
                
                if last_timestamp:
                    self.logger.info(f"Incremental record count (since {last_timestamp}): {count:,}")
                else:
                    self.logger.info(f"Full record count: {count:,}")
                return count
        except Exception as e:
            self.logger.warning(f"Could not fetch accurate record count, using fallback estimate: {e}")
            # Fallback to simple count if complex query fails
            try:
                with self.db2_conn.get_connection(log_connection=False) as conn:
                    cursor = conn.cursor()
                    fallback_query = "SELECT COUNT(*) FROM TREASURY_MM_DEAL WHERE DEAL_OPERATION = 'B' AND STATUS != 'C'"
                    if last_timestamp:
                        fallback_query += f" AND TMSTAMP > '{last_timestamp}'"
                    cursor.execute(fallback_query)
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                    self.logger.info(f"Fallback estimated record count: {count:,}")
                    return count
            except Exception as e2:
                self.logger.warning(f"Fallback count also failed: {e2}")
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
        """Setup RabbitMQ queue for interbank loans payable with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='interbank_loans_payable_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='interbank_loans_payable_dead_letter', durable=True)
            channel.queue_bind(
                queue='interbank_loans_payable_dead_letter',
                exchange='interbank_loans_payable_dlx',
                routing_key='interbank_loans_payable_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='interbank_loans_payable_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'interbank_loans_payable_dlx',
                        'x-dead-letter-routing-key': 'interbank_loans_payable_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments
                self.logger.warning(
                    "Queue 'interbank_loans_payable_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='interbank_loans_payable_queue', durable=True)
                self.logger.info("RabbitMQ queue 'interbank_loans_payable_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single interbank loans payable record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            # Map the fields from the SQL query to the dataclass
            record = InterbankLoansPayableRecord(
                reportingDate=safe_string(row[0]),                      # VARCHAR_FORMAT(mm.TMSTAMP,'DDMMYYYYHHMM')
                lenderName=safe_string(row[1]),                         # cb.BANK_NAME
                accountNumber=safe_string(row[2]),                      # linked PROFITS_ACCOUNT.ACCOUNT_NUMBER
                lenderCountry=safe_string(row[3]),                      # COALESCE(cl.COUNTRY_NAME, cb.CNTRY_ISO_CODE)
                borrowingType=safe_string(row[4]),                      # CASE mm.DEAL_OPERATION
                transactionDate=safe_string(row[5]),                    # VARCHAR_FORMAT(mm.DEAL_DATE,'DDMMYYYYHHMM')
                disbursementDate=safe_string(row[6]),                   # VARCHAR_FORMAT(mm.VALUE_DATE,'DDMMYYYYHHMM')
                maturityDate=safe_string(row[7]),                       # VARCHAR_FORMAT(mm.MATURITY_DATE,'DDMMYYYYHHMM')
                currency=safe_string(row[8]),                           # cur.SHORT_DESCR
                orgAmountOpening=safe_string(row[9]),                   # mm.SOURCE_AMOUNT
                usdAmountOpening=safe_string(row[10]),                  # USD conversion
                tzsAmountOpening=safe_string(row[11]),                  # TZS conversion
                orgAmountRepayment=safe_string(row[12]),                # COALESCE(repay.org_repayment, 0)
                usdAmountRepayment=safe_string(row[13]),                # USD repayment conversion
                tzsAmountRepayment=safe_string(row[14]),                # TZS repayment conversion
                orgAmountClosing=safe_string(row[15]),                  # Opening - Repayment
                usdAmountClosing=safe_string(row[16]),                  # USD closing conversion
                tzsAmountClosing=safe_string(row[17]),                  # TZS closing conversion
                tenureDays=safe_string(row[18]) if row[18] else None,   # COALESCE(mm.DURATION0, DAYS(...))
                annualInterestRate=safe_string(row[19]),                # DECIMAL(mm.INTEREST_RATE,15,2)
                interestRateType=safe_string(row[20]),                  # CASE mm.PREDETERMINE_FLAG
                dealNumber=safe_string(row[21])                          # mm.DEAL_NO
            )
            
            # Extract TMSTAMP for state tracking (row[22] is mm.TMSTAMP)
            tmstamp = row[22] if len(row) > 22 else None
            
            return record, tmstamp
            
        except Exception as e:
            self.logger.error(f"Error processing interbank loans payable record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate interbank loans payable record"""
        try:
            # Basic validation
            if not record.accountNumber:
                self.logger.warning("Missing account number (deal number)")
                return False
            
            if not record.lenderName:
                self.logger.warning("Missing lender name")
                return False
            
            if not record.currency:
                self.logger.warning("Missing currency")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating interbank loans payable record: {e}")
            return False
    
    def producer_thread(self, incremental=True):
        """Producer thread - executes query ONCE and streams results via fetchmany()"""
        try:
            self.logger.info("Producer thread started")
            
            # Get last successful run timestamp for incremental processing
            last_timestamp = None
            if incremental:
                last_timestamp = self.get_last_successful_run()
                if last_timestamp:
                    self.logger.info(f"Incremental mode: Processing records since {last_timestamp}")
                else:
                    self.logger.info("Incremental mode: No previous run found, processing all records")
            else:
                self.logger.info("Full load mode: Processing all records")
            
            # Get dynamic record count
            self.total_available = self.get_total_count(last_timestamp)
            
            self.logger.info(f"Total interbank loans payable records available: {self.total_available:,}")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()
            
            # Execute the query ONCE and stream results
            query = self.get_interbank_loans_payable_query(last_timestamp)
            self.logger.info("Executing interbank loans payable query (single execution, streaming results)...")
            
            latest_timestamp = None
            
            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                db2_cursor = db2_conn.cursor()
                db2_cursor.execute(query)
                self.logger.info("Query executed successfully, streaming results via fetchmany()...")
                
                batch_number = 1
                last_progress_report = time.time()
                total_fetched = 0
                
                while True:
                    batch_start_time = time.time()
                    
                    # Fetch next chunk from the already-running query
                    rows = db2_cursor.fetchmany(self.batch_size)
                    
                    if not rows:
                        self.logger.info("No more records to fetch")
                        break
                    
                    total_fetched += len(rows)
                    self.logger.debug(f"Fetched {len(rows)} rows in batch {batch_number}, total fetched: {total_fetched}")
                    
                    # Process and publish
                    batch_published = 0
                    for row in rows:
                        try:
                            record, tmstamp = self.process_record(row)
                            
                            # Track latest timestamp for state update
                            if tmstamp and (not latest_timestamp or tmstamp > latest_timestamp):
                                latest_timestamp = tmstamp
                            
                            if self.validate_record(record):
                                message = json.dumps(asdict(record), default=str)
                                
                                # Publish with retry
                                published = False
                                for attempt in range(self.max_retries):
                                    try:
                                        channel.basic_publish(
                                            exchange='',
                                            routing_key='interbank_loans_payable_queue',
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
                            else:
                                self.logger.debug(f"Record validation failed for account: {record.accountNumber if hasattr(record, 'accountNumber') else 'unknown'}")
                        except Exception as e:
                            self.logger.error(f"Error processing row in batch {batch_number}: {e}")
                            continue
                    
                    batch_time = time.time() - batch_start_time
                    with self._stats_lock:
                        produced = self.total_produced
                    
                    # Dynamic progress calculation - adjust total if we exceed the estimate
                    if produced > self.total_available and self.total_available > 0:
                        # We've exceeded the estimate, adjust it upward
                        self.total_available = max(self.total_available, produced + len(rows))
                        self.logger.debug(f"Adjusted total estimate to {self.total_available:,} based on actual data")
                    
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
            
            # Update state with latest timestamp if incremental and we processed records
            if incremental and latest_timestamp and self.total_produced > 0:
                self._update_state(latest_timestamp)
                self.logger.info(f"Updated pipeline state with latest timestamp: {latest_timestamp}")
            
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
            insert_buffer: List[InterbankLoansPayableRecord] = []
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
                    record = InterbankLoansPayableRecord(**record_data)
                    
                    insert_buffer.append(record)
                    pending_tags.append(method.delivery_tag)
                    
                    # Flush if buffer is full or time interval exceeded
                    if len(insert_buffer) >= self.consumer_batch_size or \
                       time.time() - last_flush_time >= flush_interval:
                        flush_buffer(ch)
                    
                    # Progress monitoring (only log every 1000 records, not every message)
                    with self._stats_lock:
                        consumed = self.total_consumed
                    
                    if consumed > 0 and consumed % 1000 == 0:
                        elapsed_time = time.time() - self.start_time
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        
                        # Use the maximum of estimated total or actual consumed to prevent >100%
                        effective_total = max(self.total_available, consumed)
                        progress_percent = (consumed / effective_total * 100) if effective_total > 0 else 0
                        
                        self.logger.info(f"Consumer: Processed {consumed:,} records ({progress_percent:.2f}% of {effective_total:,} total) - Rate: {rate:.1f} rec/sec")
                    
                    # Detailed progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        with self._stats_lock:
                            consumed = self.total_consumed
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        
                        # Use effective total to prevent >100% progress
                        effective_total = max(self.total_available, consumed)
                        remaining_records = effective_total - consumed if effective_total > consumed else 0
                        eta_seconds = remaining_records / rate if rate > 0 and remaining_records > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"CONSUMER PROGRESS: {consumed:,}/{effective_total:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS to match consumer batch size for efficient batching
            channel.basic_qos(prefetch_count=self.consumer_batch_size)
            channel.basic_consume(queue='interbank_loans_payable_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
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
                        queue_state = channel.queue_declare(queue='interbank_loans_payable_queue', durable=True, passive=True)
                        # `message_count` only covers ready messages. Do not close the
                        # connection while this consumer still has delivered messages
                        # awaiting a batch acknowledgement.
                        with self._stats_lock:
                            produced = self.total_produced
                            consumed = self.total_consumed
                        # Pika can still have delivery events buffered locally after
                        # RabbitMQ reports no ready messages. Keep pumping events until
                        # every published message has reached a completed batch.
                        if (
                            queue_state.method.message_count == 0
                            and not pending_tags
                            and consumed >= produced
                        ):
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
                    channel.basic_consume(queue='interbank_loans_payable_queue', on_message_callback=process_message)
            
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
    
    def insert_batch_to_postgres(self, records: List[InterbankLoansPayableRecord], pg_conn):
        """Batch insert interbank loans payable records keyed by the treasury deal number."""
        try:
            cursor = pg_conn.cursor()

            # Multiple deals may share one settlement account. Deduplicate only by
            # the unique treasury deal number, keeping the last source row if needed.
            records_by_deal = {}
            for record in records:
                records_by_deal[record.dealNumber] = record
            unique_records = list(records_by_deal.values())

            duplicate_count = len(records) - len(unique_records)
            if duplicate_count:
                self.logger.info(
                    f"Deduplicated {duplicate_count:,} repeated dealNumber value(s) "
                    f"from a {len(records):,}-record insert batch"
                )
            
            insert_query = """
            INSERT INTO "interbankLoansPayable" (
                "reportingDate", "lenderName", "accountNumber", "dealNumber", "lenderCountry",
                "borrowingType", "transactionDate", "disbursementDate", "maturityDate",
                "currency", "orgAmountOpening", "usdAmountOpening", "tzsAmountOpening",
                "orgAmountRepayment", "usdAmountRepayment", "tzsAmountRepayment",
                "orgAmountClosing", "usdAmountClosing", "tzsAmountClosing",
                "tenureDays", "annualInterestRate", "interestRateType"
            ) VALUES %s
            ON CONFLICT ("dealNumber") DO UPDATE SET
                "reportingDate" = EXCLUDED."reportingDate",
                "lenderName" = EXCLUDED."lenderName",
                "accountNumber" = EXCLUDED."accountNumber",
                "lenderCountry" = EXCLUDED."lenderCountry",
                "borrowingType" = EXCLUDED."borrowingType",
                "transactionDate" = EXCLUDED."transactionDate",
                "disbursementDate" = EXCLUDED."disbursementDate",
                "maturityDate" = EXCLUDED."maturityDate",
                "currency" = EXCLUDED."currency",
                "orgAmountOpening" = EXCLUDED."orgAmountOpening",
                "usdAmountOpening" = EXCLUDED."usdAmountOpening",
                "tzsAmountOpening" = EXCLUDED."tzsAmountOpening",
                "orgAmountRepayment" = EXCLUDED."orgAmountRepayment",
                "usdAmountRepayment" = EXCLUDED."usdAmountRepayment",
                "tzsAmountRepayment" = EXCLUDED."tzsAmountRepayment",
                "orgAmountClosing" = EXCLUDED."orgAmountClosing",
                "usdAmountClosing" = EXCLUDED."usdAmountClosing",
                "tzsAmountClosing" = EXCLUDED."tzsAmountClosing",
                "tenureDays" = EXCLUDED."tenureDays",
                "annualInterestRate" = EXCLUDED."annualInterestRate",
                "interestRateType" = EXCLUDED."interestRateType"
            """
            
            values = [
                (
                    r.reportingDate, r.lenderName, r.accountNumber, r.dealNumber, r.lenderCountry,
                    r.borrowingType, r.transactionDate, r.disbursementDate, r.maturityDate,
                    r.currency, r.orgAmountOpening, r.usdAmountOpening, r.tzsAmountOpening,
                    r.orgAmountRepayment, r.usdAmountRepayment, r.tzsAmountRepayment,
                    r.orgAmountClosing, r.usdAmountClosing, r.tzsAmountClosing,
                    r.tenureDays, r.annualInterestRate, r.interestRateType
                )
                for r in unique_records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} interbank loans payable records: {e}")
            raise
    
    def ensure_unique_index(self):
        """Ensure the treasury deal identifier is unique for upsert processing."""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_interbankloans_deal_number_unique
                    ON "interbankLoansPayable" ("dealNumber")
                """)
                conn.commit()
                self.logger.info("Unique index on dealNumber verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on dealNumber: {e}")
            raise
    
    def run_streaming_pipeline(self, incremental=True):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        mode_desc = "incremental" if incremental else "full load"
        self.logger.info(f"Starting Interbank Loans Payable STREAMING pipeline ({mode_desc})...")
        
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
            
            # Start producer thread with incremental flag
            producer_thread = threading.Thread(target=lambda: self.producer_thread(incremental), name="Producer")
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
            
            # Final statistics with corrected totals
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            
            # Use actual processed count for final statistics
            actual_total = max(self.total_produced, self.total_consumed)
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Interbank Loans Payable Pipeline Summary:
            ==========================================
            Mode: {mode_desc}
            Estimated records: {self.total_available:,}
            Actual records processed: {actual_total:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/3600:.2f} hours ({total_time:.1f} seconds)
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Interbank Loans Payable Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = InterbankLoansPayableStreamingPipeline(batch_size=args.batch_size)
    
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
