#!/usr/bin/env python3
"""
Personal Data Corporates Streaming Pipeline - Producer and Consumer run simultaneously
Based on personal-data-corporates-v4.sql query
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
class PersonalDataCorporateRecord:
    """Data class for corporate personal data records based on personal-data-corporates-v4.sql"""
    reportingDate: str
    companyName: str
    customerIdentificationNumber: str
    establishedDate: Optional[str]
    legalForm: str
    negativeClientStatus: str
    numberOfEmployees: Optional[int]
    totalEmployeesMAle: int
    totalEmployeesFemale: int
    registrationCountry: str
    registrationNumber: Optional[str]
    taxIdentificationNumber: Optional[str]
    tradeName: str
    parentName: Optional[str]
    parentIncorporationNumber: Optional[str]
    groupId: Optional[str]
    sectorSnaClassification: str
    related_customers: Optional[str]
    street: Optional[str]
    country: Optional[str]
    region: str
    district: Optional[str]
    ward: Optional[str]
    houseNumber: Optional[str]
    postalCode: Optional[str]
    poBox: Optional[str]
    zipCode: Optional[str]
    secondaryStreet: Optional[str]
    secondartHouseNumber: Optional[str]
    secondaryPostalCode: Optional[str]
    secondaryRegion: Optional[str]
    secondaryDistrict: Optional[str]
    secondaryCountry: Optional[str]
    secondaryTextAddress: Optional[str]
    mobileNumber: Optional[str]
    alternativeMobileNumber: Optional[str]
    fixedLineNumber: Optional[str]
    faxNumber: Optional[str]
    emailAddress: Optional[str]
    socialMedia: Optional[str]
    entityName: str
    entityType: str
    certificateIncorporation: Optional[str]
    entiryRegion: str
    entityDistrict: Optional[str]
    entityWard: str
    entityStreet: str
    entityHouseNumber: str
    entityPostalCode: str
    groupParentCode: str
    shareOwnedPercentage: Optional[str]
    shareOwnedAmount: Optional[str]


class PersonalDataCorporatesStreamingPipeline:
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
        
        self.logger.info("Personal Data Corporates STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_personal_data_corporates_query(self):
        """Get the corporate personal data query from personal-data-corporates-v4.sql"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'personal-data-corporates-v4.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def get_total_count(self):
        """Get approximate total count of corporate records from DB2"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM W_DIM_CUSTOMER WHERE CUST_TYPE_IND = 'Corporate'")
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Estimated record count from W_DIM_CUSTOMER (Corporate): {count:,}")
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
        """Setup RabbitMQ queue for corporate personal data with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='personal_data_corporates_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='personal_data_corporates_dead_letter', durable=True)
            channel.queue_bind(
                queue='personal_data_corporates_dead_letter',
                exchange='personal_data_corporates_dlx',
                routing_key='personal_data_corporates_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='personal_data_corporates_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'personal_data_corporates_dlx',
                        'x-dead-letter-routing-key': 'personal_data_corporates_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments
                self.logger.warning(
                    "Queue 'personal_data_corporates_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='personal_data_corporates_queue', durable=True)
                self.logger.info("RabbitMQ queue 'personal_data_corporates_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    
    def process_record(self, row):
        """Process a single corporate personal data record from DB2"""
        try:
            def safe_string(value):
                if value is None:
                    return None
                return str(value).strip()
            
            def safe_int(value):
                if value is None:
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None
            
            # Map the 52 fields from the SQL query
            record = PersonalDataCorporateRecord(
                reportingDate=safe_string(row[0]),
                companyName=safe_string(row[1]),
                customerIdentificationNumber=safe_string(row[2]),
                establishedDate=safe_string(row[3]) if row[3] else None,
                legalForm=safe_string(row[4]),
                negativeClientStatus=safe_string(row[5]),
                numberOfEmployees=safe_int(row[6]),
                totalEmployeesMAle=safe_int(row[7]),
                totalEmployeesFemale=safe_int(row[8]),
                registrationCountry=safe_string(row[9]),
                registrationNumber=safe_string(row[10]) if row[10] else None,
                taxIdentificationNumber=safe_string(row[11]) if row[11] else None,
                tradeName=safe_string(row[12]),
                parentName=safe_string(row[13]) if row[13] else None,
                parentIncorporationNumber=safe_string(row[14]) if row[14] else None,
                groupId=safe_string(row[15]) if row[15] else None,
                sectorSnaClassification=safe_string(row[16]),
                related_customers=safe_string(row[17]) if row[17] else None,
                street=safe_string(row[18]) if row[18] else None,
                country=safe_string(row[19]) if row[19] else None,
                region=safe_string(row[20]),
                district=safe_string(row[21]) if row[21] else None,
                ward=safe_string(row[22]) if row[22] else None,
                houseNumber=safe_string(row[23]) if row[23] else None,
                postalCode=safe_string(row[24]) if row[24] else None,
                poBox=safe_string(row[25]) if row[25] else None,
                zipCode=safe_string(row[26]) if row[26] else None,
                secondaryStreet=safe_string(row[27]) if row[27] else None,
                secondartHouseNumber=safe_string(row[28]) if row[28] else None,
                secondaryPostalCode=safe_string(row[29]) if row[29] else None,
                secondaryRegion=safe_string(row[30]) if row[30] else None,
                secondaryDistrict=safe_string(row[31]) if row[31] else None,
                secondaryCountry=safe_string(row[32]) if row[32] else None,
                secondaryTextAddress=safe_string(row[33]) if row[33] else None,
                mobileNumber=safe_string(row[34]) if row[34] else None,
                alternativeMobileNumber=safe_string(row[35]) if row[35] else None,
                fixedLineNumber=safe_string(row[36]) if row[36] else None,
                faxNumber=safe_string(row[37]) if row[37] else None,
                emailAddress=safe_string(row[38]) if row[38] else None,
                socialMedia=safe_string(row[39]) if row[39] else None,
                entityName=safe_string(row[40]),
                entityType=safe_string(row[41]),
                certificateIncorporation=safe_string(row[42]) if row[42] else None,
                entiryRegion=safe_string(row[43]),
                entityDistrict=safe_string(row[44]) if row[44] else None,
                entityWard=safe_string(row[45]),
                entityStreet=safe_string(row[46]),
                entityHouseNumber=safe_string(row[47]),
                entityPostalCode=safe_string(row[48]),
                groupParentCode=safe_string(row[49]),
                shareOwnedPercentage=safe_string(row[50]) if row[50] else None,
                shareOwnedAmount=safe_string(row[51]) if row[51] else None
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing corporate record: {e}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate corporate record"""
        try:
            if not record.customerIdentificationNumber:
                self.logger.warning("Missing customer identification number")
                return False
            if not record.companyName:
                self.logger.warning("Missing company name")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error validating record: {e}")
            return False
    
    def producer_thread(self):
        """Producer thread"""
        try:
            self.logger.info("Producer thread started")
            self.total_available = self.get_total_count()
            self.logger.info(f"Total corporate records available: {self.total_available:,} (estimated)")
            
            rmq_connection, channel = self.setup_rabbitmq_connection()
            query = self.get_personal_data_corporates_query()
            self.logger.info("Executing corporate query...")
            
            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                db2_cursor = db2_conn.cursor()
                db2_cursor.execute(query)
                self.logger.info("Query executed, streaming results...")
                
                batch_number = 1
                last_progress_report = time.time()
                
                while True:
                    batch_start_time = time.time()
                    rows = db2_cursor.fetchmany(self.batch_size)
                    
                    if not rows:
                        break
                    
                    batch_published = 0
                    for row in rows:
                        record = self.process_record(row)
                        if self.validate_record(record):
                            message = json.dumps(asdict(record), default=str)
                            published = False
                            for attempt in range(self.max_retries):
                                try:
                                    channel.basic_publish(
                                        exchange='',
                                        routing_key='personal_data_corporates_queue',
                                        body=message,
                                        properties=pika.BasicProperties(delivery_mode=2)
                                    )
                                    published = True
                                    break
                                except Exception as e:
                                    self.logger.warning(f"Publish attempt {attempt + 1} failed: {e}")
                                    if attempt < self.max_retries - 1:
                                        try:
                                            rmq_connection.close()
                                        except Exception:
                                            pass
                                        rmq_connection, channel = self.setup_rabbitmq_connection()
                                        time.sleep(self.retry_delay)
                            
                            if published:
                                batch_published += 1
                                with self._stats_lock:
                                    self.total_produced += 1
                    
                    batch_time = time.time() - batch_start_time
                    with self._stats_lock:
                        produced = self.total_produced
                    progress_percent = produced / self.total_available * 100 if self.total_available > 0 else 0
                    
                    self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} rows, {batch_published} published ({progress_percent:.2f}%, {batch_time:.1f}s)")
                    
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        rate = produced / elapsed_time if elapsed_time > 0 else 0
                        self.logger.info(f"PROGRESS: {produced:,}/{self.total_available:,} ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec")
                        last_progress_report = current_time
                    
                    batch_number += 1
            
            rmq_connection.close()
            with self._stats_lock:
                produced = self.total_produced
            self.logger.info(f"Producer finished: {produced:,} records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()

    
    def consumer_thread(self):
        """Consumer thread"""
        pg_conn = None
        try:
            self.logger.info("Consumer thread started")
            pg_conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            self.logger.info("Consumer: PostgreSQL connection established")
            
            connection, channel = self.setup_rabbitmq_connection()
            insert_buffer: List[PersonalDataCorporateRecord] = []
            pending_tags: List[int] = []
            last_flush_time = time.time()
            flush_interval = 5
            
            def flush_buffer(ch):
                nonlocal insert_buffer, pending_tags, last_flush_time, pg_conn
                if not insert_buffer:
                    return
                
                batch_size = len(insert_buffer)
                try:
                    self.insert_batch_to_postgres(insert_buffer, pg_conn)
                    if pending_tags:
                        ch.basic_ack(delivery_tag=pending_tags[-1], multiple=True)
                    with self._stats_lock:
                        self.total_consumed += batch_size
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
                except Exception as e:
                    self.logger.error(f"Batch insert failed: {e}")
                    try:
                        pg_conn.rollback()
                    except Exception:
                        pass
                    for tag in pending_tags:
                        try:
                            ch.basic_nack(delivery_tag=tag, requeue=False)
                        except Exception:
                            pass
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal insert_buffer, pending_tags, last_flush_time
                try:
                    record_data = json.loads(body)
                    record = PersonalDataCorporateRecord(**record_data)
                    insert_buffer.append(record)
                    pending_tags.append(method.delivery_tag)
                    
                    if len(insert_buffer) >= self.consumer_batch_size or time.time() - last_flush_time >= flush_interval:
                        flush_buffer(ch)
                except Exception as e:
                    self.logger.error(f"Consumer error: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_qos(prefetch_count=self.consumer_batch_size)
            channel.basic_consume(queue='personal_data_corporates_queue', on_message_callback=process_message)
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    if insert_buffer and time.time() - last_flush_time >= flush_interval:
                        flush_buffer(channel)
                    
                    if self.producer_finished.is_set():
                        flush_buffer(channel)
                        queue_state = channel.queue_declare(queue='personal_data_corporates_queue', durable=True, passive=True)
                        if queue_state.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty")
                            break
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records")
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
    
    def insert_batch_to_postgres(self, records: List[PersonalDataCorporateRecord], pg_conn):
        """Batch insert corporate records to PostgreSQL"""
        try:
            cursor = pg_conn.cursor()
            insert_query = """
            INSERT INTO "personalDataCorporates" (
                "reportingDate", "companyName", "customerIdentificationNumber", "establishedDate",
                "legalForm", "negativeClientStatus", "numberOfEmployees", "totalEmployeesMAle",
                "totalEmployeesFemale", "registrationCountry", "registrationNumber", "taxIdentificationNumber",
                "tradeName", "parentName", "parentIncorporationNumber", "groupId", "sectorSnaClassification",
                "related_customers", "street", "country", "region", "district", "ward", "houseNumber",
                "postalCode", "poBox", "zipCode", "secondaryStreet", "secondartHouseNumber",
                "secondaryPostalCode", "secondaryRegion", "secondaryDistrict", "secondaryCountry",
                "secondaryTextAddress", "mobileNumber", "alternativeMobileNumber", "fixedLineNumber",
                "faxNumber", "emailAddress", "socialMedia", "entityName", "entityType",
                "certificateIncorporation", "entiryRegion", "entityDistrict", "entityWard",
                "entityStreet", "entityHouseNumber", "entityPostalCode", "groupParentCode",
                "shareOwnedPercentage", "shareOwnedAmount"
            ) VALUES %s
            ON CONFLICT ("customerIdentificationNumber") DO NOTHING
            """
            
            values = [(
                r.reportingDate, r.companyName, r.customerIdentificationNumber, r.establishedDate,
                r.legalForm, r.negativeClientStatus, r.numberOfEmployees, r.totalEmployeesMAle,
                r.totalEmployeesFemale, r.registrationCountry, r.registrationNumber, r.taxIdentificationNumber,
                r.tradeName, r.parentName, r.parentIncorporationNumber, r.groupId, r.sectorSnaClassification,
                r.related_customers, r.street, r.country, r.region, r.district, r.ward, r.houseNumber,
                r.postalCode, r.poBox, r.zipCode, r.secondaryStreet, r.secondartHouseNumber,
                r.secondaryPostalCode, r.secondaryRegion, r.secondaryDistrict, r.secondaryCountry,
                r.secondaryTextAddress, r.mobileNumber, r.alternativeMobileNumber, r.fixedLineNumber,
                r.faxNumber, r.emailAddress, r.socialMedia, r.entityName, r.entityType,
                r.certificateIncorporation, r.entiryRegion, r.entityDistrict, r.entityWard,
                r.entityStreet, r.entityHouseNumber, r.entityPostalCode, r.groupParentCode,
                r.shareOwnedPercentage, r.shareOwnedAmount
            ) for r in records]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} records: {e}")
            raise
    
    def ensure_unique_index(self):
        """Ensure unique index exists"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_personaldatacorporates_customer_id_unique
                    ON "personalDataCorporates" ("customerIdentificationNumber")
                """)
                conn.commit()
                self.logger.info("Unique index verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index: {e}")
            raise
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Personal Data Corporates STREAMING pipeline...")
        
        try:
            self.ensure_unique_index()
            self.setup_rabbitmq_queue()
            
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            time.sleep(1)
            
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()
            
            producer_thread.join()
            self.logger.info("Producer completed")
            
            consumer_thread.join(timeout=60)
            if consumer_thread.is_alive():
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)
            
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Personal Data Corporates Pipeline Summary:
            ==========================================
            Total available: {self.total_available:,}
            Produced: {self.total_produced:,}
            Consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Time: {total_time/3600:.2f} hours
            Rate: {avg_rate:.1f} rec/sec
            ==========================================
            """)
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Personal Data Corporates Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--consumer-batch-size', type=int, default=100)
    args = parser.parse_args()
    
    pipeline = PersonalDataCorporatesStreamingPipeline(batch_size=args.batch_size, consumer_batch_size=args.consumer_batch_size)
    
    try:
        pipeline.run_streaming_pipeline()
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
