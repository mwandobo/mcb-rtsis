#!/usr/bin/env python3
"""
Personal Data Streaming Pipeline - Producer and Consumer run simultaneously
Uses personal_data_information-v2.sql query with camelCase naming
"""

import pika
import psycopg2
import json
import logging
import threading
import time
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from db2_connection import DB2Connection
from processors.personal_data_processor import PersonalDataProcessor, PersonalDataRecord

class PersonalDataStreamingPipeline:
    def __init__(self, batch_size=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.personal_data_processor = PersonalDataProcessor()
        self.batch_size = batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("👤 Personal Data STREAMING Pipeline initialized")
        self.logger.info(f"📊 Batch size: {batch_size} records per batch")
        self.logger.info("🔄 Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_personal_data_query(self, last_cust_id=None):
        """Get the personal data query using cursor-based pagination for DB2"""
        
        if last_cust_id is None:
            # First batch
            where_clause = ""
        else:
            # Subsequent batches - use cursor pagination
            where_clause = f"AND c.cust_id > '{last_cust_id}'"
        
        query = f"""
        SELECT CURRENT_TIMESTAMP                                                                                AS reportingDate,
               TRIM(c.cust_id)                                                                                  AS customerIdentificationNumber,
               c.first_name                                                                                     AS firstName,
               c.middle_name                                                                                    AS middleNames,
               c.surname                                                                                        AS otherNames,
               TRIM(
                       CASE
                           WHEN c.cust_type = '1' THEN
                               TRIM(NVL(c.first_name, '')) || ' ' ||
                               TRIM(NVL(c.middle_name, '')) || ' ' ||
                               TRIM(NVL(c.surname, ''))
                           WHEN c.cust_type = '2' THEN TRIM(c.surname)
                           ELSE ''
                           END
               )                                                                                                AS fullNames,

               c.surname                                                                                        AS presentSurname,
               c.surname                                                                                        AS birthSurname,
               CASE
                   WHEN c.sex = 'M' THEN 'Male'
                   WHEN c.sex = 'F' THEN 'Female'
                   ELSE 'Not Applicable'
                   END                                                                                          AS gender,

               CASE UPPER(TRIM(gd_family.description))
                   WHEN 'MARRIED' THEN 'Married'
                   WHEN 'SINGLE' THEN 'Single'
                   WHEN 'DIVORCED' THEN 'Divorced'
                   WHEN 'WIDOWED' THEN 'Widowed'
                   ELSE 'Single'
                   END                                                                                          AS maritalStatus,
               NULL                                                                                             AS numberSpouse,
               CASE UPPER(TRIM(gd_natio.description)) WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF' END  AS nationality,
               CASE UPPER(TRIM(gd_natio.description)) WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF' END  AS citizenship,
               CASE
                   WHEN c.non_resident = '0' THEN 'Resident'
                   ELSE 'Non-Resident'
                   END                                                                                          AS residency,

               gd_proff.description                                                                             AS profession,
               'Households'                                                                                     AS sectorSnaClassification,
               CASE c.CUST_STATUS
                   WHEN '5' THEN 'Disappeared'
                   ELSE 'No Fate'
                   END                                                                                          AS fateStatus,
               'N/A'                                                                                            AS socialStatus,
               CASE UPPER(TRIM(gd_employment.description))
                   WHEN 'EMPLOYED' THEN 'Employed'
                   WHEN 'SALARIED' THEN 'Employed'
                   WHEN 'CUSTOMER SERVICE' THEN 'Self-employed'
                   ELSE 'Unemployed'
                   END                                                                                          AS employmentStatus,
               c.salary_amn                                                                                     AS monthlyIncome,
               (c.num_of_children + c.children_above18)                                                         AS numberDependants,
               gd_edulevel.description                                                                          AS educationLevel,
               0.00                                                                                             AS averageMonthlyExpenditure,
               c.blacklisted_ind                                                                                AS negativeClientStatus,
               c.spouse_name                                                                                    AS spousesFullName,
               NULL                                                                                             AS spouseIdentificationType,
               NULL                                                                                             AS spouseIdentificationNumber,
               NULL                                                                                             AS maidenName,
               NULL                                                                                             AS monthlyExpenses,
               c.date_of_birth                                                                                  AS birthDate,
               id_country.description                                                                           AS birthCountry,
               NULL                                                                                             AS birthPostalCode,
               NULL                                                                                             AS birthHouseNumber,
               'N/A'                                                                                            AS birthRegion,
               'N/A'                                                                                            AS birthDistrict,
               NULL                                                                                             AS birthWard,
               NULL                                                                                             AS birthStreet,
               CASE UPPER(TRIM(idt.description))
                   WHEN 'COMPANYS REGISTRY NUMBER' THEN 'Certificate of Registration'
                   WHEN 'DRIVING LICENCE' THEN 'DrivingLicense'
                   WHEN 'NATIONAL IDENTITY CARD' THEN 'NationalIdentityCard'
                   WHEN 'PASSPORT' THEN 'Passport'
                   WHEN 'STUDENT ID' THEN 'Student ID'
                   WHEN 'VOTERS ID' THEN 'VotersRegistrationCard'
                   ELSE 'N/A'
                   END                                                                                          AS identificationType,
               id.id_no                                                                                         AS identificationNumber,
               CASE
                   WHEN id.issue_date = DATE '0001-01-01'
                       THEN 'N/A'
                   ELSE TO_CHAR(id.issue_date, 'YYYY-MM-DD')
                   END                                                                                          AS issuance_date,
               CASE
                   WHEN id.expiry_date = DATE '0001-01-01'
                       THEN 'N/A'
                   ELSE TO_CHAR(id.expiry_date, 'YYYY-MM-DD')
                   END                                                                                          AS expirationDate,
               'N/A'                                                                                            AS issuancePlace,

               CASE UPPER(TRIM(idt.description))
                   WHEN 'COMPANYS REGISTRY NUMBER' THEN 'Business Registrations and Licensing Agency (BRELA)'
                   WHEN 'DRIVING LICENCE' THEN 'Tanzania Revenue Authority (TRA)'
                   WHEN 'NATIONAL IDENTITY CARD' THEN 'National Identification Authority (NIDA)'
                   WHEN 'PASSPORT' THEN 'Immigration Services Department'
                   WHEN 'STUDENT ID' THEN 'Recognized Education Institution'
                   WHEN 'VOTERS ID' THEN 'Independent National Electoral Commission (INEC)'
                   END                                                                                          AS issuingAuthority,
               NULL                                                                                             AS businessName,
               NULL                                                                                             AS establishmentDate,
               NULL                                                                                             AS businessRegistrationNumber,
               NULL                                                                                             AS businessRegistrationDate,
               NULL                                                                                             AS businessLicenseNumber,
               NULL                                                                                             AS taxIdentificationNumber,
               NULL                                                                                             AS employerName,
               NULL                                                                                             AS employerRegion,
               NULL                                                                                             AS employerDistrict,
               NULL                                                                                             AS employerWard,
               NULL                                                                                             AS employerStreet,
               NULL                                                                                             AS employerHouseNumber,
               NULL                                                                                             AS employerPostalCode,
               NULL                                                                                             AS businessNature,
               c.mobile_tel                                                                                     AS mobileNumber,
               c.mobile_tel2                                                                                    AS alternativeMobileNumber,
               c.telephone_1                                                                                    AS fixedLineNumber,
               c_address.fax_no                                                                                 AS faxNumber,
               c.e_mail                                                                                         AS emailAddress,
               c.internet_address                                                                               AS socialMedia,
               c_address.address_1 || ' ' || c_address.address_2                                                AS mainAddress,
               NULL                                                                                             AS street,
               NULL                                                                                             AS houseNumber,
               c_address.zip_code                                                                               AS postalCode,
               c_address.CITY                                                                                   AS region,
               c_address.REGION                                                                                 AS district,
               c_address.ADDRESS_1                                                                              AS ward,
               c_country.description                                                                            AS country,
               NULL                                                                                             AS sstreet,
               NULL                                                                                             AS shouseNumber,
               NULL                                                                                             AS spostalCode,
               NULL                                                                                             AS sregion,
               NULL                                                                                             AS sdistrict,
               NULL                                                                                             AS sward,
               NULL                                                                                             AS scountry,
               
               -- Add cust_id for cursor tracking
               c.cust_id                                                                                        AS cust_id

        FROM customer c
                 LEFT JOIN cust_address c_address
                           ON c_address.fk_customercust_id = c.cust_id
                               AND c_address.communication_addr = '1'
                               AND c_address.entry_status = '1'

                 LEFT JOIN generic_detail c_country
                           ON c_address.fkgd_has_country = c_country.serial_num
                               AND c_address.fkgh_has_country = c_country.fk_generic_headpar

                 LEFT JOIN other_id id
                           ON id.fk_customercust_id = c.cust_id
                               AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END) = '1'

                 LEFT JOIN generic_detail id_country
                           ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                               AND id.fkgd_has_been_issu = id_country.serial_num

                 LEFT JOIN generic_detail idt
                           ON idt.fk_generic_headpar = id.fkgh_has_type
                               AND idt.serial_num = id.fkgd_has_type

                 LEFT JOIN customer_category cc_family
                           ON cc_family.fk_customercust_id = c.cust_id
                               AND cc_family.fk_categorycategor = 'FAMILY'
                               AND cc_family.fk_generic_detafk = 'FALST'

                 LEFT JOIN generic_detail gd_family
                           ON gd_family.fk_generic_headpar = cc_family.fk_generic_detafk
                               AND gd_family.serial_num = cc_family.fk_generic_detaser

                 LEFT JOIN customer_category cc_employment
                           ON cc_employment.fk_customercust_id = c.cust_id
                               AND cc_employment.fk_categorycategor = 'PROFLEVL'
                               AND cc_employment.fk_generic_detafk = 'PRFST'

                 LEFT JOIN generic_detail gd_employment
                           ON gd_employment.fk_generic_headpar = cc_employment.fk_generic_detafk
                               AND gd_employment.serial_num = cc_employment.fk_generic_detaser

                 LEFT JOIN customer_category cc_natio
                           ON cc_natio.fk_customercust_id = c.cust_id
                               AND cc_natio.fk_categorycategor = 'NATIONAL'
                               AND cc_natio.fk_generic_detafk = 'NATIO'

                 LEFT JOIN generic_detail gd_natio
                           ON gd_natio.fk_generic_headpar = cc_natio.fk_generic_detafk
                               AND gd_natio.serial_num = cc_natio.fk_generic_detaser

                 LEFT JOIN customer_category cc_citiz
                           ON cc_citiz.fk_customercust_id = c.cust_id
                               AND cc_citiz.fk_categorycategor = 'CITIZEN'
                               AND cc_citiz.fk_generic_detafk = 'CITIZ'

                 LEFT JOIN generic_detail gd_citiz
                           ON gd_citiz.fk_generic_headpar = cc_citiz.fk_generic_detafk
                               AND gd_citiz.serial_num = cc_citiz.fk_generic_detaser

                 LEFT JOIN customer_category cc_proff
                           ON cc_proff.fk_customercust_id = c.cust_id
                               AND cc_proff.fk_categorycategor = 'PROFES'
                               AND cc_proff.fk_generic_detafk = 'PROFF'

                 LEFT JOIN generic_detail gd_proff
                           ON gd_proff.fk_generic_headpar = cc_proff.fk_generic_detafk
                               AND gd_proff.serial_num = cc_proff.fk_generic_detaser

                 LEFT JOIN customer_category cc_edulevel
                           ON cc_edulevel.fk_customercust_id = c.cust_id
                               AND cc_edulevel.fk_categorycategor = 'EDULEVEL'
                               AND cc_edulevel.fk_generic_detafk = 'EDULV'

                 LEFT JOIN generic_detail gd_edulevel
                           ON gd_edulevel.fk_generic_headpar = cc_edulevel.fk_generic_detafk
                               AND gd_edulevel.serial_num = cc_edulevel.fk_generic_detaser
        WHERE UPPER(TRIM(idt.description)) NOT IN ('OTHER TYPE OF IDENTIFICATION', 'BIRTH CERTIFICATE') 
            AND c.CUST_TYPE = '1'
            {where_clause}
        ORDER BY c.cust_id ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available personal data records"""
        
        query = """
        SELECT COUNT(*) as total_count
        FROM customer c
        WHERE c.CUST_TYPE = '1'
        """
        
        return query
    
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
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for personal data"""
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
            
            # Declare personal data queue first
            channel.queue_declare(queue='personal_data_queue', durable=True)
            
            # Then try to purge existing queue
            try:
                channel.queue_purge('personal_data_queue')
                self.logger.info("🧹 Purged existing queue")
            except:
                pass
            
            connection.close()
            self.logger.info("✅ RabbitMQ personal data queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - fetches personal data and publishes to queue"""
        try:
            # Skip count query for now - just start processing
            self.logger.info("🏭 Producer thread started")
            self.logger.info("📊 Processing personal data records in batches...")
            
            # Process batches - cursor-based approach for large dataset
            batch_number = 1
            processed_count = 0
            last_cust_id = None
            
            while True:  # Process until no more data
                # Setup RabbitMQ connection for each batch to avoid timeouts
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
                        blocked_connection_timeout=300  # 5 minutes timeout
                    )
                    connection = pika.BlockingConnection(parameters)
                    channel = connection.channel()
                    
                    # Fetch batch using cursor
                    with self.db2_conn.get_connection() as conn:
                        cursor = conn.cursor()
                        batch_query = self.get_personal_data_query(last_cust_id)
                        cursor.execute(batch_query)
                        rows = cursor.fetchall()
                    
                    if not rows:
                        connection.close()
                        break
                    
                    self.logger.info(f"🏭 Producer: Batch {batch_number} - {len(rows)} personal data records")
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample personal data from first batch:")
                        for i, row in enumerate(rows[:3], 1):
                            cust_id = row[1] if row[1] is not None else "N/A"
                            first_name = row[2] if row[2] is not None else "N/A"
                            surname = row[4] if row[4] is not None else "N/A"
                            self.logger.info(f"  {i}. Customer: {cust_id}, Name: {first_name} {surname}")
                    
                    # Process and publish batch
                    batch_published = 0
                    for row in rows:
                        record = self.personal_data_processor.process_record(row, 'personalData')
                        
                        if self.personal_data_processor.validate_record(record):
                            message = json.dumps(asdict(record), default=str)
                            channel.basic_publish(
                                exchange='',
                                routing_key='personal_data_queue',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                            self.total_produced += 1
                            batch_published += 1
                    
                    # Close connection after batch
                    connection.close()
                    
                    # Update cursor for next batch (use last record's cust_id)
                    if rows:
                        last_row = rows[-1]
                        last_cust_id = last_row[78]  # cust_id (last column)
                        
                        self.logger.info(f"🔄 Cursor updated: last_cust_id={last_cust_id}")
                    
                    self.logger.info(f"🏭 Producer: Published batch {batch_number} ({batch_published} records, {self.total_produced} total)")
                    
                    processed_count += len(rows)
                    batch_number += 1
                    
                    # Break if we got less than batch_size (end of data)
                    if len(rows) < self.batch_size:
                        break
                    
                    # Small delay between batches
                    time.sleep(0.5)
                    
                except Exception as batch_error:
                    self.logger.error(f"❌ Batch {batch_number} error: {batch_error}")
                    # Continue to next batch
                    processed_count += self.batch_size  # Skip this batch
                    batch_number += 1
                    time.sleep(2)  # Wait before retry
            
            self.logger.info(f"🏭 Producer finished: {self.total_produced} personal data records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Producer error: {e}")
            import traceback
            traceback.print_exc()
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes personal data messages from queue"""
        try:
            self.logger.info("👤 Consumer thread started")
            
            # Setup RabbitMQ connection for consumer with better timeout settings
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials,
                heartbeat=600,  # 10 minutes heartbeat
                blocked_connection_timeout=300  # 5 minutes timeout
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = PersonalDataRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.personal_data_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    self.total_consumed += 1
                    
                    if self.total_consumed % self.batch_size == 0:
                        self.logger.info(f"👤 Consumer: Processed {self.total_consumed} personal data records")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Consumer error processing personal data message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=5)  # Process 5 messages at a time
            channel.basic_consume(queue='personal_data_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        try:
                            method = channel.queue_declare(queue='personal_data_queue', durable=True, passive=True)
                            if method.method.message_count == 0:
                                self.logger.info("👤 Consumer: Queue empty, producer finished")
                                break
                        except:
                            # If queue check fails, assume we should stop
                            break
                        
                except Exception as e:
                    self.logger.error(f"❌ Consumer processing error: {e}")
                    # Try to reconnect
                    try:
                        connection.close()
                    except:
                        pass
                    time.sleep(2)
                    try:
                        connection = pika.BlockingConnection(parameters)
                        channel = connection.channel()
                        channel.basic_qos(prefetch_count=5)
                        channel.basic_consume(queue='personal_data_queue', on_message_callback=process_message)
                    except Exception as reconnect_error:
                        self.logger.error(f"❌ Failed to reconnect: {reconnect_error}")
                        break
            
            try:
                connection.close()
            except:
                pass
            self.logger.info(f"👤 Consumer finished: {self.total_consumed} personal data records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            import traceback
            traceback.print_exc()
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("🚀 Starting STREAMING personal data pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="PersonalData-Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="PersonalData-Producer")
            producer_thread.start()
            
            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("✅ Producer thread completed")
            
            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=30)  # 30 second timeout
            
            if consumer_thread.is_alive():
                self.logger.warning("⚠️ Consumer thread timeout, stopping...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=5)
            
            self.logger.info("✅ Consumer thread completed")
            
            self.logger.info(f"📊 STREAMING Personal Data Pipeline Results:")
            self.logger.info(f"   Produced: {self.total_produced:,} records")
            self.logger.info(f"   Consumed: {self.total_consumed:,} records")
            
            return self.total_consumed
            
        except Exception as e:
            self.logger.error(f"❌ Streaming personal data pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("👤 PERSONAL DATA STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Minimal queue accumulation")
    print("  - Batch size: 10 records per batch")
    print("  - camelCase table: personalData")
    print("  - camelCase field names")
    print("  - Uses personal_data_information-v2.sql query")
    print("  - Customer-based cursor pagination")
    print("=" * 60)
    
    pipeline = PersonalDataStreamingPipeline(10)
    
    try:
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ STREAMING PERSONAL DATA PIPELINE COMPLETED!")
        print(f"📊 Total personal data records processed: {count:,}")
        print("🔍 Key advantages:")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Memory efficient")
        print("  - Fast processing")
        print("  - camelCase naming throughout")
        print("  - Unique customer identification")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Streaming personal data pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()