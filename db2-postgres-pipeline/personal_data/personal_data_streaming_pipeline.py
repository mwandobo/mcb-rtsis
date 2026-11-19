#!/usr/bin/env python3
"""
Personal Data Streaming Pipeline - Producer and Consumer run simultaneously
Based on personal_data_information-v4.sql query
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
class PersonalDataRecord:
    """Data class for personal data records based on personal_data_information-v4.sql"""
    reportingDate: str
    customerIdentificationNumber: str
    firstName: str
    middleNames: str
    otherNames: str
    fullNames: str
    presentSurname: str
    birthSurname: str
    gender: str
    maritalStatus: str
    numberSpouse: Optional[str]
    nationality: Optional[str]
    citizenship: Optional[str]
    residency: str
    profession: Optional[str]
    sectorSnaClassification: str
    fateStatus: str
    socialStatus: str
    employmentStatus: str
    monthlyIncome: Optional[str]
    numberDependants: Optional[int]
    educationLevel: str
    averageMonthlyExpenditure: str
    negativeClientStatus: Optional[str]
    spousesFullName: Optional[str]
    spouseIdentificationType: Optional[str]
    spouseIdentificationNumber: Optional[str]
    maidenName: Optional[str]
    monthlyExpenses: Optional[str]
    birthDate: Optional[str]
    birthCountry: Optional[str]
    birthPostalCode: Optional[str]
    birthHouseNumber: Optional[str]
    birthRegion: str
    birthDistrict: Optional[str]
    birthWard: Optional[str]
    birthStreet: Optional[str]
    identificationType: str
    identificationNumber: Optional[str]
    issuance_date: Optional[str]
    expirationDate: Optional[str]
    issuancePlace: str
    issuingAuthority: Optional[str]
    businessName: Optional[str]
    establishmentDate: Optional[str]
    businessRegistrationNumber: Optional[str]
    businessRegistrationDate: Optional[str]
    businessLicenseNumber: Optional[str]
    taxIdentificationNumber: Optional[str]
    employerName: Optional[str]
    employerRegion: Optional[str]
    employerDistrict: Optional[str]
    employerWard: Optional[str]
    employerStreet: Optional[str]
    employerHouseNumber: Optional[str]
    employerPostalCode: Optional[str]
    businessNature: Optional[str]
    mobileNumber: Optional[str]
    alternativeMobileNumber: Optional[str]
    fixedLineNumber: Optional[str]
    faxNumber: Optional[str]
    emailAddress: Optional[str]
    socialMedia: Optional[str]
    mainAddress: Optional[str]
    street: Optional[str]
    houseNumber: Optional[str]
    postalCode: Optional[str]
    region: str
    district: Optional[str]
    ward: Optional[str]
    country: Optional[str]
    sstreet: Optional[str]
    shouseNumber: Optional[str]
    spostalCode: Optional[str]
    sregion: Optional[str]
    sdistrict: Optional[str]
    sward: Optional[str]
    scountry: Optional[str]


class PersonalDataStreamingPipeline:
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
        
        self.logger.info("Personal Data STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_personal_data_query(self, last_timestamp=None):
        """Get the personal data query from person.sql, with optional TMSTAMP filter for incremental mode"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'person.sql'
        )

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read().strip()

        if sql.endswith(';'):
            sql = sql[:-1]

        # Query already has a WHERE clause with only AND-chained conditions
        # (no OR at the top level), so it's safe to just AND the filter on.
        if last_timestamp:
            if last_timestamp == 'epoch':
                timestamp_value = '1900-01-01-00.00.00.000000'
            else:
                timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]

            sql = sql + f"\nAND c.TMSTAMP > '{timestamp_value}'"

        return sql

    def get_last_successful_run(self):
        """Get last successful run timestamp from state manager"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            return state_manager.get_last_successful_run('personal_data')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None

    def get_total_count(self):
        """Get approximate total count of personal data records from DB2"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM CUSTOMER WHERE CUST_TYPE = '1'")
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Estimated record count from CUSTOMER: {count:,}")
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
        """Setup RabbitMQ queue for personal data with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='personal_data_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='personal_data_dead_letter', durable=True)
            channel.queue_bind(
                queue='personal_data_dead_letter',
                exchange='personal_data_dlx',
                routing_key='personal_data_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='personal_data_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'personal_data_dlx',
                        'x-dead-letter-routing-key': 'personal_data_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments
                self.logger.warning(
                    "Queue 'personal_data_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='personal_data_queue', durable=True)
                self.logger.info("RabbitMQ queue 'personal_data_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    
    def process_record(self, row):
        """Process a single personal data record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            def safe_int(value):
                """Safely convert to int"""
                if value is None:
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None
            
            # Map the fields from the SQL query to the dataclass (72 fields)
            record = PersonalDataRecord(
                reportingDate=safe_string(row[0]),
                customerIdentificationNumber=safe_string(row[1]),
                firstName=safe_string(row[2]),
                middleNames=safe_string(row[3]),
                otherNames=safe_string(row[4]),
                fullNames=safe_string(row[5]),
                presentSurname=safe_string(row[6]),
                birthSurname=safe_string(row[7]),
                gender=safe_string(row[8]),
                maritalStatus=safe_string(row[9]),
                numberSpouse=safe_string(row[10]) if row[10] else None,
                nationality=safe_string(row[11]) if row[11] else None,
                citizenship=safe_string(row[12]) if row[12] else None,
                residency=safe_string(row[13]),
                profession=safe_string(row[14]) if row[14] else None,
                sectorSnaClassification=safe_string(row[15]),
                fateStatus=safe_string(row[16]),
                socialStatus=safe_string(row[17]),
                employmentStatus=safe_string(row[18]),
                monthlyIncome=safe_string(row[19]) if row[19] else None,
                numberDependants=safe_int(row[20]),
                educationLevel=safe_string(row[21]),
                averageMonthlyExpenditure=safe_string(row[22]),
                negativeClientStatus=safe_string(row[23]) if row[23] else None,
                spousesFullName=safe_string(row[24]) if row[24] else None,
                spouseIdentificationType=safe_string(row[25]) if row[25] else None,
                spouseIdentificationNumber=safe_string(row[26]) if row[26] else None,
                maidenName=safe_string(row[27]) if row[27] else None,
                monthlyExpenses=safe_string(row[28]) if row[28] else None,
                birthDate=safe_string(row[29]) if row[29] else None,
                birthCountry=safe_string(row[30]) if row[30] else None,
                birthPostalCode=safe_string(row[31]) if row[31] else None,
                birthHouseNumber=safe_string(row[32]) if row[32] else None,
                birthRegion=safe_string(row[33]),
                birthDistrict=safe_string(row[34]) if row[34] else None,
                birthWard=safe_string(row[35]) if row[35] else None,
                birthStreet=safe_string(row[36]) if row[36] else None,
                identificationType=safe_string(row[37]),
                identificationNumber=safe_string(row[38]) if row[38] else None,
                issuance_date=safe_string(row[39]) if row[39] else None,
                expirationDate=safe_string(row[40]) if row[40] else None,
                issuancePlace=safe_string(row[41]),
                issuingAuthority=safe_string(row[42]) if row[42] else None,
                businessName=safe_string(row[43]) if row[43] else None,
                establishmentDate=safe_string(row[44]) if row[44] else None,
                businessRegistrationNumber=safe_string(row[45]) if row[45] else None,
                businessRegistrationDate=safe_string(row[46]) if row[46] else None,
                businessLicenseNumber=safe_string(row[47]) if row[47] else None,
                taxIdentificationNumber=safe_string(row[48]) if row[48] else None,
                employerName=safe_string(row[49]) if row[49] else None,
                employerRegion=safe_string(row[50]) if row[50] else None,
                employerDistrict=safe_string(row[51]) if row[51] else None,
                employerWard=safe_string(row[52]) if row[52] else None,
                employerStreet=safe_string(row[53]) if row[53] else None,
                employerHouseNumber=safe_string(row[54]) if row[54] else None,
                employerPostalCode=safe_string(row[55]) if row[55] else None,
                businessNature=safe_string(row[56]) if row[56] else None,
                mobileNumber=safe_string(row[57]) if row[57] else None,
                alternativeMobileNumber=safe_string(row[58]) if row[58] else None,
                fixedLineNumber=safe_string(row[59]) if row[59] else None,
                faxNumber=safe_string(row[60]) if row[60] else None,
                emailAddress=safe_string(row[61]) if row[61] else None,
                socialMedia=safe_string(row[62]) if row[62] else None,
                mainAddress=safe_string(row[63]) if row[63] else None,
                street=safe_string(row[64]) if row[64] else None,
                houseNumber=safe_string(row[65]) if row[65] else None,
                postalCode=safe_string(row[66]) if row[66] else None,
                region=safe_string(row[67]),
                district=safe_string(row[68]) if row[68] else None,
                ward=safe_string(row[69]) if row[69] else None,
                country=safe_string(row[70]) if row[70] else None,
                sstreet=safe_string(row[71]) if row[71] else None,
                shouseNumber=safe_string(row[72]) if row[72] else None,
                spostalCode=safe_string(row[73]) if row[73] else None,
                sregion=safe_string(row[74]) if row[74] else None,
                sdistrict=safe_string(row[75]) if row[75] else None,
                sward=safe_string(row[76]) if row[76] else None,
                scountry=safe_string(row[77]) if row[77] else None
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing personal data record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate personal data record"""
        try:
            # Basic validation
            if not record.customerIdentificationNumber:
                self.logger.warning("Missing customer identification number")
                return False
            
            if not record.fullNames:
                self.logger.warning("Missing full names")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating personal data record: {e}")
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

            self.logger.info(f"Total personal data records available: {self.total_available:,} (estimated)")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")

            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()

            # Execute the query ONCE and stream results
            query = self.get_personal_data_query(last_timestamp)
            self.logger.info("Executing personal data query (single execution, streaming results)...")
            
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
                                        routing_key='personal_data_queue',
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
            insert_buffer: List[PersonalDataRecord] = []
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
                    record = PersonalDataRecord(**record_data)
                    
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
            channel.basic_consume(queue='personal_data_queue', on_message_callback=process_message)
            
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
                        queue_state = channel.queue_declare(queue='personal_data_queue', durable=True, passive=True)
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
                    channel.basic_consume(queue='personal_data_queue', on_message_callback=process_message)
            
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
    
    def insert_batch_to_postgres(self, records: List[PersonalDataRecord], pg_conn):
        """Batch insert personal data records to PostgreSQL with duplicate prevention"""
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "personalDataInformation" (
                "reportingDate", "customerIdentificationNumber", "firstName", "middleNames",
                "otherNames", "fullNames", "presentSurname", "birthSurname", "gender",
                "maritalStatus", "numberSpouse", "nationality", "citizenship", "residency",
                "profession", "sectorSnaClassification", "fateStatus", "socialStatus",
                "employmentStatus", "monthlyIncome", "numberDependants", "educationLevel",
                "averageMonthlyExpenditure", "negativeClientStatus", "spousesFullName",
                "spouseIdentificationType", "spouseIdentificationNumber", "maidenName",
                "monthlyExpenses", "birthDate", "birthCountry", "birthPostalCode",
                "birthHouseNumber", "birthRegion", "birthDistrict", "birthWard", "birthStreet",
                "identificationType", "identificationNumber", "issuance_date", "expirationDate",
                "issuancePlace", "issuingAuthority", "businessName", "establishmentDate",
                "businessRegistrationNumber", "businessRegistrationDate", "businessLicenseNumber",
                "taxIdentificationNumber", "employerName", "employerRegion", "employerDistrict",
                "employerWard", "employerStreet", "employerHouseNumber", "employerPostalCode",
                "businessNature", "mobileNumber", "alternativeMobileNumber", "fixedLineNumber",
                "faxNumber", "emailAddress", "socialMedia", "mainAddress", "street",
                "houseNumber", "postalCode", "region", "district", "ward", "country",
                "sstreet", "shouseNumber", "spostalCode", "sregion", "sdistrict", "sward", "scountry"
            ) VALUES %s
            ON CONFLICT ("customerIdentificationNumber") DO NOTHING
            """
            
            values = [
                (
                    r.reportingDate, r.customerIdentificationNumber, r.firstName, r.middleNames,
                    r.otherNames, r.fullNames, r.presentSurname, r.birthSurname, r.gender,
                    r.maritalStatus, r.numberSpouse, r.nationality, r.citizenship, r.residency,
                    r.profession, r.sectorSnaClassification, r.fateStatus, r.socialStatus,
                    r.employmentStatus, r.monthlyIncome, r.numberDependants, r.educationLevel,
                    r.averageMonthlyExpenditure, r.negativeClientStatus, r.spousesFullName,
                    r.spouseIdentificationType, r.spouseIdentificationNumber, r.maidenName,
                    r.monthlyExpenses, r.birthDate, r.birthCountry, r.birthPostalCode,
                    r.birthHouseNumber, r.birthRegion, r.birthDistrict, r.birthWard, r.birthStreet,
                    r.identificationType, r.identificationNumber, r.issuance_date, r.expirationDate,
                    r.issuancePlace, r.issuingAuthority, r.businessName, r.establishmentDate,
                    r.businessRegistrationNumber, r.businessRegistrationDate, r.businessLicenseNumber,
                    r.taxIdentificationNumber, r.employerName, r.employerRegion, r.employerDistrict,
                    r.employerWard, r.employerStreet, r.employerHouseNumber, r.employerPostalCode,
                    r.businessNature, r.mobileNumber, r.alternativeMobileNumber, r.fixedLineNumber,
                    r.faxNumber, r.emailAddress, r.socialMedia, r.mainAddress, r.street,
                    r.houseNumber, r.postalCode, r.region, r.district, r.ward, r.country,
                    r.sstreet, r.shouseNumber, r.spostalCode, r.sregion, r.sdistrict, r.sward, r.scountry
                )
                for r in records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} personal data records: {e}")
            raise
    
    def ensure_unique_index(self):
        """Ensure unique index on customerIdentificationNumber exists for ON CONFLICT duplicate prevention"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_personaldatainformation_customer_id_unique
                    ON "personalDataInformation" ("customerIdentificationNumber")
                """)
                conn.commit()
                self.logger.info("Unique index on customerIdentificationNumber verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on customerIdentificationNumber: {e}")
            raise
    
    def run_streaming_pipeline(self, incremental=True):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info(f"Starting Personal Data STREAMING pipeline... (incremental={incremental})")

        try:
            self._update_state('running')

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
            Personal Data Pipeline Summary:
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
            state_manager.update_run('personal_data', status, self.total_consumed, error_message)
            self.logger.info(f"State updated: {status}, records: {self.total_consumed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Personal Data Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for DB2 query pagination')
    parser.add_argument('--consumer-batch-size', type=int, default=100, help='Batch size for PostgreSQL inserts')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')

    args = parser.parse_args()

    # Create pipeline
    pipeline = PersonalDataStreamingPipeline(batch_size=args.batch_size, consumer_batch_size=args.consumer_batch_size)

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
