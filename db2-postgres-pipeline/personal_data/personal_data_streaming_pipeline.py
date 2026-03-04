#!/usr/bin/env python3
"""
Personal Data Streaming Pipeline - Producer and Consumer run simultaneously
Uses personal_data_information-v3.sql query for personal data extraction

Improvements over original pipeline:
  1. Single query execution + fetchmany() streaming (no re-execute per batch)
  2. Thread-safe counters with _stats_lock
  3. Batch inserts via psycopg2.extras.execute_values
  4. ON CONFLICT duplicate prevention on customerIdentificationNumber
  5. Consumer batch buffering with flush interval
  6. Dead-letter queue support with graceful fallback
  7. Persistent PostgreSQL connection in consumer
  8. Unique index enforcement at startup
  9. Module-level logging.basicConfig
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

# Configure logging at module level (should only be called once)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class PersonalDataRecord:
    """Data class for personal data records based on personal_data_information-v3.sql"""
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
    nationality: str
    citizenship: str
    residency: str
    profession: Optional[str]
    sectorSnaClassification: str
    fateStatus: str
    socialStatus: str
    employmentStatus: str
    monthlyIncome: Optional[str]
    numberDependants: Optional[int]
    educationLevel: str
    averageMonthlyExpenditure: Optional[float]
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
    birthRegion: Optional[str]
    birthDistrict: Optional[str]
    birthWard: Optional[str]
    birthStreet: Optional[str]
    identificationType: str
    identificationNumber: Optional[str]
    issuance_date: Optional[str]
    expirationDate: Optional[str]
    issuancePlace: Optional[str]
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
    region: Optional[str]
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
    def __init__(self, batch_size=1000, consumer_batch_size=100, sql_version='v3'):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = consumer_batch_size
        self.sql_version = sql_version

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

        self.logger.info(f"Personal Data STREAMING Pipeline initialized (SQL version: {self.sql_version})")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")

    def get_personal_data_query(self):
        """Get the full personal data query from personal_data_information-v3.sql or v4.sql.

        The query is executed ONCE and results are streamed via cursor.fetchmany().
        No pagination modifications are made to the SQL.

        Returns:
            str: The complete SQL query
        """
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', f'personal_data_information-{self.sql_version}.sql'
        )
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                query = file.read().strip()
                self.logger.info(f"Loaded personal data query from: {sql_file_path}")
                return query
        except FileNotFoundError:
            self.logger.error(f"SQL file not found: {sql_file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading SQL file: {e}")
            raise

    def validate_record(self, record_data):
        """Validate a personal data record before processing.
        
        Args:
            record_data: Dictionary containing record data
            
        Returns:
            bool: True if record is valid, False otherwise
        """
        try:
            # Check required fields
            if not record_data.get('customerIdentificationNumber'):
                self.logger.warning("Record missing customerIdentificationNumber")
                return False
                
            if not record_data.get('fullNames'):
                self.logger.warning("Record missing fullNames")
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Error validating record: {e}")
            return False

    @contextmanager
    def setup_rabbitmq_connection(self):
        """Setup RabbitMQ connection with proper error handling"""
        connection = None
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
            yield connection, channel
        except Exception as e:
            self.logger.error(f"RabbitMQ connection failed: {e}")
            raise
        finally:
            if connection and not connection.is_closed:
                connection.close()

    def setup_rabbitmq_queues(self):
        """Setup RabbitMQ queues with dead-letter support"""
        with self.setup_rabbitmq_connection() as (connection, channel):
            self.logger.info("Setting up RabbitMQ queues...")

            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='personal_data_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='personal_data_dead_letter', durable=True)
            
            channel.queue_bind(
                queue='personal_data_dead_letter',
                exchange='personal_data_dlx',
                routing_key='personal_data_queue'
            )

            # Declare main queue with dead-letter support
            try:
                channel.queue_declare(
                    queue='personal_data_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'personal_data_dlx',
                        'x-dead-letter-routing-key': 'personal_data_queue'
                    }
                )
                self.logger.info("RabbitMQ queue 'personal_data_queue' setup complete (with DLX)")
            except pika.exceptions.ChannelClosedByBroker:
                # Queue may already exist with different arguments; reconnect and use as-is
                self.logger.warning(
                    "Queue 'personal_data_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='personal_data_queue', durable=True)
                self.logger.info("RabbitMQ queue 'personal_data_queue' setup complete (without DLX)")

            connection.close()
            self.logger.info("RabbitMQ setup completed successfully")

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
            self.logger.error(f"PostgreSQL connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def ensure_unique_index(self):
        """Ensure unique index exists on customerIdentificationNumber"""
        try:
            with self.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if index exists
                    cursor.execute("""
                        SELECT indexname FROM pg_indexes 
                        WHERE tablename = 'personal_data_information' 
                        AND indexname = 'idx_personal_data_customer_id_unique'
                    """)
                    
                    if not cursor.fetchone():
                        self.logger.info("Creating unique index on customerIdentificationNumber...")
                        cursor.execute("""
                            CREATE UNIQUE INDEX CONCURRENTLY idx_personal_data_customer_id_unique 
                            ON personal_data_information (customerIdentificationNumber)
                        """)
                        conn.commit()
                        self.logger.info("Unique index created successfully")
                    else:
                        self.logger.info("Unique index already exists")
        except Exception as e:
            self.logger.warning(f"Could not create unique index: {e}")

    def producer_thread(self):
        """Producer thread - reads from DB2 and publishes to RabbitMQ"""
        try:
            self.logger.info("Producer: Starting...")
            
            with self.setup_rabbitmq_connection() as (connection, channel):
                # Execute the v3 query ONCE and stream results
                query = self.get_personal_data_query()
                
                self.logger.info("Executing personal data query (single execution, streaming results)...")
                
                with self.db2_conn.get_connection() as db2_conn:
                    cursor = db2_conn.cursor()
                    cursor.execute(query)
                    
                    batch_count = 0
                    total_records = 0
                    
                    while True:
                        # Fetch batch using fetchmany for memory efficiency
                        rows = cursor.fetchmany(self.batch_size)
                        if not rows:
                            break
                            
                        batch_count += 1
                        batch_records = []
                        
                        for row in rows:
                            try:
                                # Convert row to dictionary using column names
                                columns = [desc[0].lower() for desc in cursor.description]
                                record_dict = dict(zip(columns, row))
                                
                                # Convert dates and handle None values
                                for key, value in record_dict.items():
                                    if value is None:
                                        record_dict[key] = None
                                    elif isinstance(value, datetime):
                                        record_dict[key] = value.isoformat()
                                    else:
                                        record_dict[key] = str(value).strip() if isinstance(value, str) else value
                                
                                # Validate record
                                if self.validate_record(record_dict):
                                    batch_records.append(record_dict)
                                
                            except Exception as e:
                                self.logger.error(f"Error processing row: {e}")
                                continue
                        
                        # Publish batch to RabbitMQ
                        if batch_records:
                            for record in batch_records:
                                message = json.dumps(record, default=str)
                                channel.basic_publish(
                                    exchange='',
                                    routing_key='personal_data_queue',
                                    body=message,
                                    properties=pika.BasicProperties(delivery_mode=2)
                                )
                            
                            total_records += len(batch_records)
                            
                            with self._stats_lock:
                                self.total_produced += len(batch_records)
                            
                            self.logger.info(f"Producer: Batch {batch_count} - {len(batch_records)} records published (Total: {total_records})")
                    
                    self.logger.info(f"Producer: Completed - {total_records} total records published")
                    
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            raise
        finally:
            self.producer_finished.set()
            self.logger.info("Producer: Finished")

    def consumer_thread(self):
        """Consumer thread - consumes from RabbitMQ and inserts to PostgreSQL"""
        buffer = []
        last_flush_time = time.time()
        flush_interval = 30  # seconds
        
        try:
            self.logger.info("Consumer: Starting...")
            
            # Maintain persistent PostgreSQL connection
            with self.get_postgres_connection() as pg_conn:
                pg_conn.autocommit = False
                
                def flush_buffer(force=False):
                    nonlocal buffer, last_flush_time
                    
                    if not buffer:
                        return
                    
                    current_time = time.time()
                    should_flush = (
                        len(buffer) >= self.consumer_batch_size or
                        force or
                        (current_time - last_flush_time) >= flush_interval
                    )
                    
                    if should_flush:
                        try:
                            with pg_conn.cursor() as cursor:
                                # Prepare insert query with ON CONFLICT
                                insert_query = """
                                    INSERT INTO personal_data_information (
                                        reportingdate, customeridentificationnumber, firstname, middlenames, 
                                        othernames, fullnames, presentsurname, birthsurname, gender, 
                                        maritalstatus, numberspouse, nationality, citizenship, residency, 
                                        profession, sectorsnaclassification, fatestatus, socialstatus, 
                                        employmentstatus, monthlyincome, numberdependants, educationlevel, 
                                        averagemonthlyexpenditure, negativeclientstatus, spousesfullname, 
                                        spouseidentificationtype, spouseidentificationnumber, maidenname, 
                                        monthlyexpenses, birthdate, birthcountry, birthpostalcode, 
                                        birthhousennumber, birthregion, birthdistrict, birthward, 
                                        birthstreet, identificationtype, identificationnumber, 
                                        issuance_date, expirationdate, issuanceplace, issuingauthority, 
                                        businessname, establishmentdate, businessregistrationnumber, 
                                        businessregistrationdate, businesslicensenumber, 
                                        taxidentificationnumber, employername, employerregion, 
                                        employerdistrict, employerward, employerstreet, 
                                        employerhousennumber, employerpostalcode, businessnature, 
                                        mobilenumber, alternativemobilenumber, fixedlinenumber, 
                                        faxnumber, emailaddress, socialmedia, mainaddress, street, 
                                        housenumber, postalcode, region, district, ward, country, 
                                        sstreet, shousennumber, spostalcode, sregion, sdistrict, 
                                        sward, scountry
                                    ) VALUES %s
                                    ON CONFLICT (customeridentificationnumber) DO UPDATE SET
                                        reportingdate = EXCLUDED.reportingdate,
                                        fullnames = EXCLUDED.fullnames,
                                        gender = EXCLUDED.gender,
                                        maritalstatus = EXCLUDED.maritalstatus,
                                        nationality = EXCLUDED.nationality,
                                        citizenship = EXCLUDED.citizenship,
                                        residency = EXCLUDED.residency,
                                        profession = EXCLUDED.profession,
                                        employmentstatus = EXCLUDED.employmentstatus,
                                        monthlyincome = EXCLUDED.monthlyincome,
                                        educationlevel = EXCLUDED.educationlevel,
                                        mobilenumber = EXCLUDED.mobilenumber,
                                        emailaddress = EXCLUDED.emailaddress,
                                        region = EXCLUDED.region,
                                        district = EXCLUDED.district,
                                        ward = EXCLUDED.ward
                                """
                                
                                # Prepare values tuple
                                values = []
                                for record in buffer:
                                    values.append((
                                        record.get('reportingdate'),
                                        record.get('customeridentificationnumber'),
                                        record.get('firstname'),
                                        record.get('middlenames'),
                                        record.get('othernames'),
                                        record.get('fullnames'),
                                        record.get('presentsurname'),
                                        record.get('birthsurname'),
                                        record.get('gender'),
                                        record.get('maritalstatus'),
                                        record.get('numberspouse'),
                                        record.get('nationality'),
                                        record.get('citizenship'),
                                        record.get('residency'),
                                        record.get('profession'),
                                        record.get('sectorsnaclassification'),
                                        record.get('fatestatus'),
                                        record.get('socialstatus'),
                                        record.get('employmentstatus'),
                                        record.get('monthlyincome'),
                                        record.get('numberdependants'),
                                        record.get('educationlevel'),
                                        record.get('averagemonthlyexpenditure'),
                                        record.get('negativeclientstatus'),
                                        record.get('spousesfullname'),
                                        record.get('spouseidentificationtype'),
                                        record.get('spouseidentificationnumber'),
                                        record.get('maidenname'),
                                        record.get('monthlyexpenses'),
                                        record.get('birthdate'),
                                        record.get('birthcountry'),
                                        record.get('birthpostalcode'),
                                        record.get('birthhousennumber'),
                                        record.get('birthregion'),
                                        record.get('birthdistrict'),
                                        record.get('birthward'),
                                        record.get('birthstreet'),
                                        record.get('identificationtype'),
                                        record.get('identificationnumber'),
                                        record.get('issuance_date'),
                                        record.get('expirationdate'),
                                        record.get('issuanceplace'),
                                        record.get('issuingauthority'),
                                        record.get('businessname'),
                                        record.get('establishmentdate'),
                                        record.get('businessregistrationnumber'),
                                        record.get('businessregistrationdate'),
                                        record.get('businesslicensenumber'),
                                        record.get('taxidentificationnumber'),
                                        record.get('employername'),
                                        record.get('employerregion'),
                                        record.get('employerdistrict'),
                                        record.get('employerward'),
                                        record.get('employerstreet'),
                                        record.get('employerhousennumber'),
                                        record.get('employerpostalcode'),
                                        record.get('businessnature'),
                                        record.get('mobilenumber'),
                                        record.get('alternativemobilenumber'),
                                        record.get('fixedlinenumber'),
                                        record.get('faxnumber'),
                                        record.get('emailaddress'),
                                        record.get('socialmedia'),
                                        record.get('mainaddress'),
                                        record.get('street'),
                                        record.get('housenumber'),
                                        record.get('postalcode'),
                                        record.get('region'),
                                        record.get('district'),
                                        record.get('ward'),
                                        record.get('country'),
                                        record.get('sstreet'),
                                        record.get('shousennumber'),
                                        record.get('spostalcode'),
                                        record.get('sregion'),
                                        record.get('sdistrict'),
                                        record.get('sward'),
                                        record.get('scountry')
                                    ))
                                
                                # Execute batch insert
                                psycopg2.extras.execute_values(cursor, insert_query, values)
                                pg_conn.commit()
                                
                                with self._stats_lock:
                                    self.total_consumed += len(buffer)
                                
                                self.logger.info(f"Consumer: Flushed {len(buffer)} records to PostgreSQL")
                                
                        except Exception as e:
                            self.logger.error(f"Error flushing buffer: {e}")
                            pg_conn.rollback()
                            raise
                        
                        buffer.clear()
                        last_flush_time = current_time

                def process_message(channel, method, properties, body):
                    try:
                        record = json.loads(body.decode('utf-8'))
                        buffer.append(record)
                        
                        # Acknowledge message
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                        
                        # Check if we should flush
                        flush_buffer()
                        
                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}")
                        # Reject message and send to dead letter queue
                        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

                # Setup consumer
                with self.setup_rabbitmq_connection() as (connection, channel):
                    # Set QoS to match consumer batch size for efficient batching
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='personal_data_queue', on_message_callback=process_message)
                    
                    # Keep consuming until producer is done and queue is empty
                    while True:
                        try:
                            connection.process_data_events(time_limit=1)
                            
                            if self.producer_finished.is_set():
                                # Producer is done, check if queue is empty
                                queue_state = channel.queue_declare(queue='personal_data_queue', durable=True, passive=True)
                                if queue_state.method.message_count == 0:
                                    self.logger.info("Consumer: Queue empty, producer finished")
                                    break
                                    
                        except pika.exceptions.AMQPConnectionError:
                            self.logger.warning("Consumer: Connection lost, reconnecting...")
                            break
                        except Exception as e:
                            self.logger.error(f"Consumer error: {e}")
                            break
                    
                    # Final flush
                    flush_buffer(force=True)
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='personal_data_queue', on_message_callback=process_message)
                    
        except Exception as e:
            self.logger.error(f"Consumer thread error: {e}")
            raise
        finally:
            self.consumer_finished.set()
            self.logger.info("Consumer: Finished")

    def run_streaming_pipeline(self):
        """Run the complete streaming pipeline"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("STARTING PERSONAL DATA STREAMING PIPELINE")
            self.logger.info("=" * 60)
            
            # Setup infrastructure
            self.setup_rabbitmq_queues()
            self.ensure_unique_index()
            
            # Start producer and consumer threads
            producer_thread = threading.Thread(target=self.producer_thread, name="PersonalDataProducer")
            consumer_thread = threading.Thread(target=self.consumer_thread, name="PersonalDataConsumer")
            
            producer_thread.start()
            consumer_thread.start()
            
            # Wait for both threads to complete
            producer_thread.join()
            consumer_thread.join()
            
            # Final statistics
            elapsed_time = time.time() - self.start_time
            with self._stats_lock:
                self.logger.info("=" * 60)
                self.logger.info("PERSONAL DATA PIPELINE COMPLETED")
                self.logger.info(f"Total produced: {self.total_produced}")
                self.logger.info(f"Total consumed: {self.total_consumed}")
                self.logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
                if elapsed_time > 0:
                    self.logger.info(f"Average rate: {self.total_consumed/elapsed_time:.2f} records/second")
                self.logger.info("=" * 60)
                
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    # Default to v4 (optimized) but allow v3 via environment variable
    sql_version = os.getenv('SQL_VERSION', 'v4')
    pipeline = PersonalDataStreamingPipeline(batch_size=1000, consumer_batch_size=100, sql_version=sql_version)
    pipeline.run_streaming_pipeline()