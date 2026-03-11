#!/usr/bin/env python3
"""
Investment Debt Securities Streaming Pipeline - Producer and Consumer run simultaneously
Based on debt-securities-investments.sql query
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
class InvestmentDebtSecuritiesRecord:
    """Data class for investment debt securities records based on investment_debt_securities.sql"""
    reportingDate: str
    securityNumber: str
    securityType: str
    securityIssuerName: str
    ratingStatus: str
    externalIssuerRating: str
    gradesUnratedBanks: Optional[str]
    securityIssuerCountry: str
    sectorSnaClassification: str
    currency: str
    orgCostValueAmount: str
    usdCostValueAmount: str
    tzsCostValueAmount: str
    faceValueAmount: str
    usdFaceValueAmount: str
    tzsFaceValueAmount: str
    orgFairValueAmount: str
    usdFairValueAmount: str
    tzsFairValueAmount: str
    interestRate: str
    purchaseDate: str
    valueDate: str
    maturityDate: str
    tradingIntent: str
    securityEncumbranceStatus: str
    pastDueDays: str
    allowanceProbableLoss: str
    botProvision: str
    assetClassificationCategory: str


class InvestmentDebtSecuritiesStreamingPipeline:
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
        
        self.logger.info("Investment Debt Securities STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_investment_debt_securities_query(self):
        """Get the investment debt securities query from investment_debt_securities.sql"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'investment_debt_securities.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def get_total_count(self):
        """Get accurate total count of investment debt securities records from DB2 using the same query structure"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                
                # Use the same query structure as the main query but just count the results
                count_query = """
                SELECT COUNT(*) FROM (
                    SELECT mm.DEAL_NO
                    FROM TRS_DEAL_RECORDING tdr
                    JOIN TREASURY_MM_DEAL mm ON tdr.DEAL_NO = mm.DEAL_NO
                    LEFT JOIN TRS_DEAL_COLLATERAL coll ON tdr.DEAL_NO = coll.FK_DEAL_NO AND coll.ENTRY_STATUS = '1'
                    LEFT JOIN CURRENCY cur ON mm.FK_SOURCE_CURRENCY = cur.ID_CURRENCY
                    LEFT JOIN COLLABORATION_BANK cb ON mm.FK_DEAL_COL_BANK = cb.BANK_ID
                    LEFT JOIN CUSTOMER cust_iss ON mm.FK_CORRESP_CUST = cust_iss.CUST_ID
                    LEFT JOIN PRODUCT prd ON tdr.ID_PRODUCT = prd.ID_PRODUCT
                    LEFT JOIN (
                        SELECT r1.CUST_ID, r1.RATING
                        FROM CUST_EXT_RATING r1
                        WHERE (r1.CUST_ID, r1.CREATE_DT, r1.RATE_DT) IN (
                            SELECT r2.CUST_ID, MAX(r2.CREATE_DT), MAX(r2.RATE_DT)
                            FROM CUST_EXT_RATING r2
                            GROUP BY r2.CUST_ID
                        )
                    ) cer ON mm.FK_CORRESP_CUST = cer.CUST_ID
                    LEFT JOIN (
                        SELECT COUNTRY_CODE, MAX(COUNTRY_NAME) AS COUNTRY_NAME
                        FROM COUNTRIES_LOOKUP
                        GROUP BY COUNTRY_CODE
                    ) cl ON cb.CNTRY_ISO_CODE = cl.COUNTRY_CODE
                    WHERE tdr.CANCEL_FLG != '1' AND tdr.DEAL_STATUS != 'C'
                    GROUP BY mm.DEAL_NO, mm.DEAL_REF_NO, mm.BOND_CODE, mm.BANK_ID_BIC, mm.FK_CORRESP_CUST,
                             mm.INTEREST_RATE, mm.DEAL_DATE, mm.VALUE_DATE, mm.MATURITY_DATE, mm.SEC_MATCHING_FLG,
                             mm.STATUS, tdr.BUY_CURRENCY, tdr.ID_PRODUCT, cur.NATIONAL_FLAG, cb.BANK_NAME,
                             cb.BANK_ID, cb.CNTRY_ISO_CODE, cb.DOM_CENTRAL_BANK, cust_iss.SURNAME, cl.COUNTRY_NAME,
                             prd.DESCRIPTION, cust_iss.CUST_TYPE, cer.RATING
                ) AS count_subquery
                """
                
                cursor.execute(count_query)
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Accurate record count from full query structure: {count:,}")
                return count
        except Exception as e:
            self.logger.warning(f"Could not fetch accurate record count, using fallback estimate: {e}")
            # Fallback to simple count if complex query fails
            try:
                with self.db2_conn.get_connection(log_connection=False) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM TRS_DEAL_RECORDING tdr
                        JOIN TREASURY_MM_DEAL mm ON tdr.DEAL_NO = mm.DEAL_NO
                        WHERE tdr.CANCEL_FLG != '1' AND tdr.DEAL_STATUS != 'C'
                    """)
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
        """Setup RabbitMQ queue for investment debt securities with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='investment_debt_securities_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='investment_debt_securities_dead_letter', durable=True)
            channel.queue_bind(
                queue='investment_debt_securities_dead_letter',
                exchange='investment_debt_securities_dlx',
                routing_key='investment_debt_securities_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='investment_debt_securities_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'investment_debt_securities_dlx',
                        'x-dead-letter-routing-key': 'investment_debt_securities_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments
                self.logger.warning(
                    "Queue 'investment_debt_securities_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='investment_debt_securities_queue', durable=True)
                self.logger.info("RabbitMQ queue 'investment_debt_securities_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single investment debt securities record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            # Map the fields from the SQL query to the dataclass (30 fields total)
            record = InvestmentDebtSecuritiesRecord(
                reportingDate=safe_string(row[0]),                      # VARCHAR_FORMAT(tdr.TMSTAMP, 'DDMMYYYYHHMM')
                securityNumber=safe_string(row[1]),                     # COALESCE(NULLIF(TRIM(coll.BOND_ISIN), ''), ...)
                securityType=safe_string(row[2]),                       # 'Treasury bonds'
                securityIssuerName=safe_string(row[3]),                 # COALESCE(TRIM(cust_iss.SURNAME), ...)
                ratingStatus=safe_string(row[4]),                       # CASE WHEN mm.FK_CORRESP_CUST IS NOT NULL...
                externalIssuerRating=safe_string(row[5]),               # CASE WHEN cb.DOM_CENTRAL_BANK = '1'...
                gradesUnratedBanks=safe_string(row[6]) if row[6] else None,  # CASE WHEN cb.DOM_CENTRAL_BANK = '1'...
                securityIssuerCountry=safe_string(row[7]),              # COALESCE(TRIM(cl.COUNTRY_NAME), ...)
                sectorSnaClassification=safe_string(row[8]),            # CASE WHEN cb.DOM_CENTRAL_BANK = '1'...
                currency=safe_string(row[9]),                           # TRIM(tdr.BUY_CURRENCY)
                orgCostValueAmount=safe_string(row[10]),                # COALESCE(NULLIF(mm.SETTLEMENT_AMT, 0), ...)
                usdCostValueAmount=safe_string(row[11]),                # USD conversion
                tzsCostValueAmount=safe_string(row[12]),                # TZS conversion
                faceValueAmount=safe_string(row[13]),                   # COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), ...)
                usdFaceValueAmount=safe_string(row[14]),                # USD face value conversion
                tzsFaceValueAmount=safe_string(row[15]),                # TZS face value conversion
                orgFairValueAmount=safe_string(row[16]),                # Fair value with market price
                usdFairValueAmount=safe_string(row[17]),                # USD fair value conversion
                tzsFairValueAmount=safe_string(row[18]),                # TZS fair value conversion
                interestRate=safe_string(row[19]),                      # DECIMAL(mm.INTEREST_RATE, 15, 2)
                purchaseDate=safe_string(row[20]),                      # VARCHAR_FORMAT(mm.DEAL_DATE, 'DDMMYYYYHHMM')
                valueDate=safe_string(row[21]),                         # VARCHAR_FORMAT(mm.VALUE_DATE, 'DDMMYYYYHHMM')
                maturityDate=safe_string(row[22]),                      # VARCHAR_FORMAT(mm.MATURITY_DATE, 'DDMMYYYYHHMM')
                tradingIntent=safe_string(row[23]),                     # CASE mm.SEC_MATCHING_FLG...
                securityEncumbranceStatus=safe_string(row[24]),         # CASE WHEN mm.STATUS = 'P'...
                pastDueDays=safe_string(row[25]),                       # CASE WHEN mm.MATURITY_DATE < CURRENT DATE...
                allowanceProbableLoss=safe_string(row[26]),             # DECIMAL(0, 15, 2)
                botProvision=safe_string(row[27]),                      # DECIMAL(0, 15, 2)
                assetClassificationCategory=safe_string(row[28])        # CASE WHEN mm.MATURITY_DATE < CURRENT DATE...
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing investment debt securities record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate investment debt securities record"""
        try:
            # Basic validation
            if not record.securityNumber:
                self.logger.warning("Missing security number")
                return False
            
            if not record.securityIssuerName:
                self.logger.warning("Missing security issuer name")
                return False
            
            if not record.currency:
                self.logger.warning("Missing currency")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating investment debt securities record: {e}")
            return False
    
    def producer_thread(self):
        """Producer thread - executes query ONCE and streams results via fetchmany()"""
        try:
            self.logger.info("Producer thread started")
            
            # Get dynamic record count
            self.total_available = self.get_total_count()
            
            self.logger.info(f"Total investment debt securities records available: {self.total_available:,} (estimated)")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()
            
            # Execute the query ONCE and stream results
            query = self.get_investment_debt_securities_query()
            self.logger.info("Executing investment debt securities query (single execution, streaming results)...")
            
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
                            record = self.process_record(row)
                            
                            if self.validate_record(record):
                                message = json.dumps(asdict(record), default=str)
                                
                                # Publish with retry
                                published = False
                                for attempt in range(self.max_retries):
                                    try:
                                        channel.basic_publish(
                                            exchange='',
                                            routing_key='investment_debt_securities_queue',
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
                                self.logger.debug(f"Record validation failed for security: {record.securityNumber if hasattr(record, 'securityNumber') else 'unknown'}")
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
            insert_buffer: List[InvestmentDebtSecuritiesRecord] = []
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
                    record = InvestmentDebtSecuritiesRecord(**record_data)
                    
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
            channel.basic_consume(queue='investment_debt_securities_queue', on_message_callback=process_message)
            
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
                        queue_state = channel.queue_declare(queue='investment_debt_securities_queue', durable=True, passive=True)
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
                    channel.basic_consume(queue='investment_debt_securities_queue', on_message_callback=process_message)
            
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
    
    def insert_batch_to_postgres(self, records: List[InvestmentDebtSecuritiesRecord], pg_conn):
        """Batch insert investment debt securities records to PostgreSQL with duplicate prevention"""
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "investmentDebtSecurities" (
                "reportingDate", "securityNumber", "securityType", "securityIssuerName",
                "ratingStatus", "externalIssuerRating", "gradesUnratedBanks", "securityIssuerCountry",
                "sectorSnaClassification", "currency", "orgCostValueAmount", "usdCostValueAmount",
                "tzsCostValueAmount", "faceValueAmount", "usdFaceValueAmount", "tzsFaceValueAmount",
                "orgFairValueAmount", "usdFairValueAmount", "tzsFairValueAmount", "interestRate",
                "purchaseDate", "valueDate", "maturityDate", "tradingIntent", "securityEncumbranceStatus",
                "pastDueDays", "allowanceProbableLoss", "botProvision", "assetClassificationCategory"
            ) VALUES %s
            ON CONFLICT ("securityNumber") DO NOTHING
            """
            
            values = [
                (
                    r.reportingDate, r.securityNumber, r.securityType, r.securityIssuerName,
                    r.ratingStatus, r.externalIssuerRating, r.gradesUnratedBanks, r.securityIssuerCountry,
                    r.sectorSnaClassification, r.currency, r.orgCostValueAmount, r.usdCostValueAmount,
                    r.tzsCostValueAmount, r.faceValueAmount, r.usdFaceValueAmount, r.tzsFaceValueAmount,
                    r.orgFairValueAmount, r.usdFairValueAmount, r.tzsFairValueAmount, r.interestRate,
                    r.purchaseDate, r.valueDate, r.maturityDate, r.tradingIntent, r.securityEncumbranceStatus,
                    r.pastDueDays, r.allowanceProbableLoss, r.botProvision, r.assetClassificationCategory
                )
                for r in records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} investment debt securities records: {e}")
            raise
    
    def ensure_unique_index(self):
        """Ensure unique index on securityNumber exists for ON CONFLICT duplicate prevention"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_investmentdebtsecurities_security_number_unique
                    ON "investmentDebtSecurities" ("securityNumber")
                """)
                conn.commit()
                self.logger.info("Unique index on securityNumber verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on securityNumber: {e}")
            raise
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Investment Debt Securities STREAMING pipeline...")
        
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
            Investment Debt Securities Pipeline Summary:
            ==========================================
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
    
    parser = argparse.ArgumentParser(description='Investment Debt Securities Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for DB2 query pagination')
    parser.add_argument('--consumer-batch-size', type=int, default=100, help='Batch size for PostgreSQL inserts')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = InvestmentDebtSecuritiesStreamingPipeline(batch_size=args.batch_size, consumer_batch_size=args.consumer_batch_size)
    
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