#!/usr/bin/env python3
"""
Agents Streaming Pipeline - Producer and Consumer run simultaneously
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

from config import Config
from db2_connection import DB2Connection

@dataclass
class AgentRecord:
    reportingDate: str
    agentName: str
    terminalID: str
    agentId: str
    tillNumber: Optional[str]
    businessForm: str
    agentPrincipal: str
    agentPrincipalName: str
    gender: str
    registrationDate: str
    closedDate: Optional[str]
    certIncorporation: Optional[str]
    nationality: str
    agentStatus: str
    agentType: str
    accountNumber: Optional[str]
    region: str
    district: str
    ward: str
    street: Optional[str]
    houseNumber: Optional[str]
    postalCode: Optional[str]
    country: str
    gpsCoordinates: Optional[str]
    agentTaxIdentificationNumber: str  # NOT NULL
    businessLicense: str  # NOT NULL

class AgentsStreamingPipeline:
    def __init__(self, batch_size=500):
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
        self.retry_delay = 2
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Agents STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_agents_query(self, last_agent_id=None):
        """Get the agents query with cursor-based pagination"""
        
        where_clause = ""
        
        if last_agent_id:
            where_clause = f"WHERE al.AGENT_ID > '{last_agent_id}'"
        
        query = f"""
        SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
               al.AGENT_NAME                                     AS agentName,
               al.TERMINAL_ID                                    AS terminalID,
               al.AGENT_ID                                       AS agentId,
               al.TILL_NUMBER                                    AS tillNumber,
               al.BUSINESS_FORM                                  AS businessForm,
               al.AGENT_PRINCIPAL                                AS agentPrincipal,
               al.AGENT_PRINCIPAL_NAME                           AS agentPrincipalName,
               al.GENDER                                         AS gender,
               al.REGISTRATION_DATE                              AS registrationDate,
               al.CLOSED_DATE                                    AS closedDate,
               al.CERT_INCORPORATION                             AS certIncorporation,
               al.NATIONALITY                                    AS nationality,
               al.AGENT_STATUS                                   AS agentStatus,
               al.AGENT_TYPE                                     AS agentType,
               al.ACCOUNT_NUMBER                                 AS accountNumber,
               al.REGION                                         AS region,
               al.DISTRICT                                       AS district,
               al.WARD                                           AS ward,
               al.STREET                                         AS street,
               al.HOUSE_NUMBER                                   AS houseNumber,
               al.POSTAL_CODE                                    AS postalCode,
               al.COUNTRY                                        AS country,
               al.GPS_COORDINATES                                AS gpsCoordinates,
               al.AGENT_TAX_IDENTIFICATION_NUMBER                AS agentTaxIdentificationNumber,
               al.BUSINESS_LICENCE                               AS businessLicense,
               al.AGENT_ID                                       AS cursor_agent_id
        FROM AGENTS_LIST_V3 al
        {where_clause}
        ORDER BY al.AGENT_ID ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available agent records"""
        return """
        SELECT COUNT(*) as total_count
        FROM AGENTS_LIST_V3
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
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                return connection, channel
                
            except Exception as e:
                self.logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for agents"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue='agents_queue', durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'agents_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def clean_date_value(self, value):
        """Clean date value - convert NIL or empty strings to None"""
        if not value:
            return None
        
        str_value = str(value).strip().upper()
        
        # Check for NIL or empty
        if str_value in ('NIL', 'NULL', 'NONE', ''):
            return None
        
        return str_value
    
    def process_record(self, row):
        """Process a single record - returns None if record should be skipped"""
        # Remove cursor field (last one)
        row_data = row[:-1]
        
        # Check if agentTaxIdentificationNumber or businessLicense are NULL/empty
        # If so, skip this record silently
        agent_tax_id = row_data[24]
        business_license = row_data[25]
        
        if not agent_tax_id or str(agent_tax_id).strip() == '' or str(agent_tax_id).strip().upper() in ('NIL', 'NULL', 'NONE'):
            return None
        
        if not business_license or str(business_license).strip() == '' or str(business_license).strip().upper() in ('NIL', 'NULL', 'NONE'):
            return None
        
        return AgentRecord(
            reportingDate=str(row_data[0]) if row_data[0] else None,
            agentName=str(row_data[1]).strip() if row_data[1] else None,
            terminalID=str(row_data[2]).strip() if row_data[2] else None,
            agentId=str(row_data[3]).strip() if row_data[3] else None,
            tillNumber=str(row_data[4]).strip() if row_data[4] else None,
            businessForm=str(row_data[5]).strip() if row_data[5] else None,
            agentPrincipal=str(row_data[6]).strip() if row_data[6] else None,
            agentPrincipalName=str(row_data[7]).strip() if row_data[7] else None,
            gender=str(row_data[8]).strip() if row_data[8] else None,
            registrationDate=self.clean_date_value(row_data[9]),
            closedDate=self.clean_date_value(row_data[10]),
            certIncorporation=str(row_data[11]).strip() if row_data[11] else None,
            nationality=str(row_data[12]).strip() if row_data[12] else None,
            agentStatus=str(row_data[13]).strip() if row_data[13] else None,
            agentType=str(row_data[14]).strip() if row_data[14] else None,
            accountNumber=str(row_data[15]).strip() if row_data[15] else None,
            region=str(row_data[16]).strip() if row_data[16] else None,
            district=str(row_data[17]).strip() if row_data[17] else None,
            ward=str(row_data[18]).strip() if row_data[18] else None,
            street=str(row_data[19]).strip() if row_data[19] else None,
            houseNumber=str(row_data[20]).strip() if row_data[20] else None,
            postalCode=str(row_data[21]).strip() if row_data[21] else None,
            country=str(row_data[22]).strip() if row_data[22] else None,
            gpsCoordinates=str(row_data[23]).strip() if row_data[23] else None,
            agentTaxIdentificationNumber=str(row_data[24]).strip(),  # NOT NULL - validated above
            businessLicense=str(row_data[25]).strip()  # NOT NULL - validated above
        )
    
    def insert_to_postgres(self, record: AgentRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "agents" (
            "reportingDate", "agentName", "terminalID", "agentId", "tillNumber",
            "businessForm", "agentPrincipal", "agentPrincipalName", gender,
            "registrationDate", "closedDate", "certIncorporation", nationality,
            "agentStatus", "agentType", "accountNumber", region, district, ward,
            street, "houseNumber", "postalCode", country, "gpsCoordinates",
            "agentTaxIdentificationNumber", "businessLicense"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("agentId") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "agentName" = EXCLUDED."agentName",
            "terminalID" = EXCLUDED."terminalID",
            "tillNumber" = EXCLUDED."tillNumber",
            "businessForm" = EXCLUDED."businessForm",
            "agentPrincipal" = EXCLUDED."agentPrincipal",
            "agentPrincipalName" = EXCLUDED."agentPrincipalName",
            gender = EXCLUDED.gender,
            "registrationDate" = EXCLUDED."registrationDate",
            "closedDate" = EXCLUDED."closedDate",
            "certIncorporation" = EXCLUDED."certIncorporation",
            nationality = EXCLUDED.nationality,
            "agentStatus" = EXCLUDED."agentStatus",
            "agentType" = EXCLUDED."agentType",
            "accountNumber" = EXCLUDED."accountNumber",
            region = EXCLUDED.region,
            district = EXCLUDED.district,
            ward = EXCLUDED.ward,
            street = EXCLUDED.street,
            "houseNumber" = EXCLUDED."houseNumber",
            "postalCode" = EXCLUDED."postalCode",
            country = EXCLUDED.country,
            "gpsCoordinates" = EXCLUDED."gpsCoordinates",
            "agentTaxIdentificationNumber" = EXCLUDED."agentTaxIdentificationNumber",
            "businessLicense" = EXCLUDED."businessLicense"
        """
        
        cursor.execute(insert_sql, (
            record.reportingDate,
            record.agentName,
            record.terminalID,
            record.agentId,
            record.tillNumber,
            record.businessForm,
            record.agentPrincipal,
            record.agentPrincipalName,
            record.gender,
            record.registrationDate,
            record.closedDate,
            record.certIncorporation,
            record.nationality,
            record.agentStatus,
            record.agentType,
            record.accountNumber,
            record.region,
            record.district,
            record.ward,
            record.street,
            record.houseNumber,
            record.postalCode,
            record.country,
            record.gpsCoordinates,
            record.agentTaxIdentificationNumber,
            record.businessLicense
        ))
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total agent records available: {self.total_available:,}")
            
            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches
            batch_number = 1
            last_agent_id = None
            last_progress_report = time.time()
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection() as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_agents_query(last_agent_id)
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
                
                # Process and publish
                batch_published = 0
                for row in rows:
                    last_agent_id = row[-1]  # cursor_agent_id
                    
                    record = self.process_record(row)
                    
                    # Skip record if it's None (validation failed)
                    if record is None:
                        continue
                    
                    message = json.dumps(asdict(record), default=str)
                    
                    # Publish
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange='',
                                routing_key='agents_queue',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                            published = True
                            break
                        except Exception as e:
                            self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                try:
                                    connection.close()
                                except:
                                    pass
                                connection, channel = self.setup_rabbitmq_connection()
                                time.sleep(self.retry_delay)
                    
                    if published:
                        batch_published += 1
                        self.total_produced += 1
                
                batch_time = time.time() - batch_start_time
                progress_percent = self.total_produced / self.total_available * 100 if self.total_available > 0 else 0
                
                self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                
                # Progress report every 5 minutes
                current_time = time.time()
                if current_time - last_progress_report >= 300:
                    elapsed_time = current_time - self.start_time
                    rate = self.total_produced / elapsed_time if elapsed_time > 0 else 0
                    remaining_records = self.total_available - self.total_produced
                    eta_seconds = remaining_records / rate if rate > 0 else 0
                    eta_minutes = eta_seconds / 60
                    
                    self.logger.info(f"PROGRESS REPORT: {self.total_produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} minutes")
                    last_progress_report = current_time
                
                batch_number += 1
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced:,} records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue"""
        try:
            self.logger.info("Consumer thread started - waiting for messages...")
            
            connection, channel = self.setup_rabbitmq_connection()
            last_progress_report = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal last_progress_report
                try:
                    record_data = json.loads(body)
                    record = AgentRecord(**record_data)
                    
                    # Insert to PostgreSQL
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
                    
                    if inserted:
                        self.total_consumed += 1
                        
                        # Log more frequently to show concurrent activity
                        if self.total_consumed % 100 == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                            self.logger.info(f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}%) - Rate: {rate:.1f} rec/sec")
                        
                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:
                            elapsed_time = current_time - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            remaining_records = self.total_available - self.total_consumed if self.total_available > 0 else 0
                            eta_seconds = remaining_records / rate if rate > 0 else 0
                            eta_minutes = eta_seconds / 60
                            
                            self.logger.info(f"CONSUMER PROGRESS: {self.total_consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} minutes")
                            last_progress_report = current_time
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue='agents_queue', on_message_callback=process_message)
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    if self.producer_finished.is_set():
                        method = channel.queue_declare(queue='agents_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    try:
                        connection.close()
                    except:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=10)
                    channel.basic_consume(queue='agents_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Agents STREAMING pipeline...")
        
        try:
            self.setup_rabbitmq_queue()
            
            # Start consumer first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            time.sleep(1)
            
            # Start producer
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()
            
            # Wait for completion
            producer_thread.join()
            self.logger.info("Producer thread completed")
            
            consumer_thread.join(timeout=60)
            
            if consumer_thread.is_alive():
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)
            
            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Agents Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/60:.2f} minutes
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Agents Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=500, help='Batch size for processing')
    
    args = parser.parse_args()
    
    pipeline = AgentsStreamingPipeline(batch_size=args.batch_size)
    
    try:
        pipeline.run_streaming_pipeline()
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
