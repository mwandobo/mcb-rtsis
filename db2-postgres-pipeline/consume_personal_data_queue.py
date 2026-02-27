#!/usr/bin/env python3
"""
Consume remaining messages from personal_data_queue and insert to PostgreSQL
Standalone consumer with retry logic and connection resilience
"""

import pika
import psycopg2
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal
from contextlib import contextmanager

from config import Config


@dataclass
class PersonalDataRecord:
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
    averageMonthlyExpenditure: Decimal
    negativeClientStatus: Optional[str]
    spousesFullName: Optional[str]
    spouseIdentificationType: Optional[str]
    spouseIdentificationNumber: Optional[str]
    maidenName: Optional[str]
    monthlyExpenses: Optional[Decimal]
    birthDate: Optional[str]
    birthCountry: Optional[str]
    birthPostalCode: Optional[str]
    birthHouseNumber: Optional[str]
    birthRegion: str
    birthDistrict: Optional[str]
    birthWard: Optional[str]
    birthStreet: Optional[str]
    identificationType: str
    identificationNumber: str
    issuanceDate: Optional[str]
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
    mainAddress: str
    street: Optional[str]
    houseNumber: Optional[str]
    postalCode: str
    region: str
    district: Optional[str]
    ward: str
    country: Optional[str]
    sstreet: Optional[str]
    shouseNumber: Optional[str]
    spostalCode: Optional[str]
    sregion: Optional[str]
    sdistrict: Optional[str]
    sward: Optional[str]
    scountry: Optional[str]


class PersonalDataQueueConsumer:
    def __init__(self):
        self.config = Config()
        self.total_consumed = 0
        self.total_failed = 0
        self.start_time = time.time()
        self.max_retries = 5
        self.retry_delay = 3
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Personal Data Queue Consumer initialized")

    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection with retry logic"""
        conn = None
        try:
            for attempt in range(self.max_retries):
                try:
                    conn = psycopg2.connect(
                        host=self.config.database.pg_host,
                        port=self.config.database.pg_port,
                        database=self.config.database.pg_database,
                        user=self.config.database.pg_user,
                        password=self.config.database.pg_password,
                    )
                    yield conn
                    return
                except Exception as e:
                    self.logger.warning(f"PostgreSQL connection attempt {attempt + 1} failed: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    else:
                        raise
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

    def setup_rabbitmq_connection(self):
        """Setup RabbitMQ connection with retry logic"""
        for attempt in range(self.max_retries):
            try:
                credentials = pika.PlainCredentials(
                    self.config.message_queue.rabbitmq_user,
                    self.config.message_queue.rabbitmq_password,
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
                
                # Declare queue to ensure it exists
                channel.queue_declare(queue="personal_data_queue", durable=True)
                
                return connection, channel
            except Exception as e:
                self.logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def insert_to_postgres(self, record: PersonalDataRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "personalData" (
            "reportingDate", "customerIdentificationNumber", "firstName", "middleNames",
            "otherNames", "fullNames", "presentSurname", "birthSurname", gender,
            "maritalStatus", "numberSpouse", nationality, citizenship, residency,
            profession, "sectorSnaClassification", "fateStatus", "socialStatus",
            "employmentStatus", "monthlyIncome", "numberDependants", "educationLevel",
            "averageMonthlyExpenditure", "negativeClientStatus", "spousesFullName",
            "spouseIdentificationType", "spouseIdentificationNumber", "maidenName",
            "monthlyExpenses", "birthDate", "birthCountry", "birthPostalCode",
            "birthHouseNumber", "birthRegion", "birthDistrict", "birthWard",
            "birthStreet", "identificationType", "identificationNumber", "issuanceDate",
            "expirationDate", "issuancePlace", "issuingAuthority", "businessName",
            "establishmentDate", "businessRegistrationNumber", "businessRegistrationDate",
            "businessLicenseNumber", "taxIdentificationNumber", "employerName",
            "employerRegion", "employerDistrict", "employerWard", "employerStreet",
            "employerHouseNumber", "employerPostalCode", "businessNature", "mobileNumber",
            "alternativeMobileNumber", "fixedLineNumber", "faxNumber", "emailAddress",
            "socialMedia", "mainAddress", street, "houseNumber", "postalCode",
            region, district, ward, country, sstreet, "shouseNumber", "spostalCode",
            sregion, sdistrict, sward, scountry
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        cursor.execute(
            insert_sql,
            (
                record.reportingDate, record.customerIdentificationNumber, record.firstName, record.middleNames,
                record.otherNames, record.fullNames, record.presentSurname, record.birthSurname, record.gender,
                record.maritalStatus, record.numberSpouse, record.nationality, record.citizenship, record.residency,
                record.profession, record.sectorSnaClassification, record.fateStatus, record.socialStatus,
                record.employmentStatus, record.monthlyIncome, record.numberDependants, record.educationLevel,
                record.averageMonthlyExpenditure, record.negativeClientStatus, record.spousesFullName,
                record.spouseIdentificationType, record.spouseIdentificationNumber, record.maidenName,
                record.monthlyExpenses, record.birthDate, record.birthCountry, record.birthPostalCode,
                record.birthHouseNumber, record.birthRegion, record.birthDistrict, record.birthWard,
                record.birthStreet, record.identificationType, record.identificationNumber, record.issuanceDate,
                record.expirationDate, record.issuancePlace, record.issuingAuthority, record.businessName,
                record.establishmentDate, record.businessRegistrationNumber, record.businessRegistrationDate,
                record.businessLicenseNumber, record.taxIdentificationNumber, record.employerName,
                record.employerRegion, record.employerDistrict, record.employerWard, record.employerStreet,
                record.employerHouseNumber, record.employerPostalCode, record.businessNature, record.mobileNumber,
                record.alternativeMobileNumber, record.fixedLineNumber, record.faxNumber, record.emailAddress,
                record.socialMedia, record.mainAddress, record.street, record.houseNumber, record.postalCode,
                record.region, record.district, record.ward, record.country, record.sstreet, record.shouseNumber,
                record.spostalCode, record.sregion, record.sdistrict, record.sward, record.scountry
            ),
        )

    def get_queue_message_count(self, channel):
        """Get the number of messages in the queue"""
        try:
            method = channel.queue_declare(queue="personal_data_queue", durable=True, passive=True)
            return method.method.message_count
        except Exception as e:
            self.logger.error(f"Failed to get queue message count: {e}")
            return 0

    def consume_queue(self):
        """Consume all messages from the queue"""
        try:
            self.logger.info("Starting queue consumer...")
            
            # Setup RabbitMQ connection
            connection, channel = self.setup_rabbitmq_connection()
            
            # Get initial queue size
            initial_queue_size = self.get_queue_message_count(channel)
            self.logger.info(f"Queue has {initial_queue_size:,} messages to process")
            
            if initial_queue_size == 0:
                self.logger.info("Queue is empty. Nothing to consume.")
                connection.close()
                return
            
            # Set prefetch to process multiple messages at once
            channel.basic_qos(prefetch_count=10)
            
            last_progress_report = time.time()
            messages_processed = 0
            
            def process_message(ch, method, properties, body):
                nonlocal last_progress_report, messages_processed
                
                try:
                    # Parse message
                    record_data = json.loads(body)
                    record = PersonalDataRecord(**record_data)
                    
                    # Insert to PostgreSQL with retry
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
                            error_msg = str(e).lower()
                            # Check if it's a duplicate key error
                            if 'duplicate' in error_msg or 'unique constraint' in error_msg:
                                self.logger.debug(f"Duplicate record skipped: {record.customerIdentificationNumber}")
                                inserted = True
                                break
                            
                            self.logger.warning(f"PostgreSQL insert attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                            else:
                                self.logger.error(f"Failed to insert record after {self.max_retries} attempts: {e}")
                    
                    if inserted:
                        self.total_consumed += 1
                        messages_processed += 1
                        
                        # Acknowledge message
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        
                        # Progress logging every 100 records
                        if self.total_consumed % 100 == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            progress_percent = (self.total_consumed / initial_queue_size * 100) if initial_queue_size > 0 else 0
                            remaining = initial_queue_size - self.total_consumed
                            eta_seconds = remaining / rate if rate > 0 else 0
                            eta_minutes = eta_seconds / 60
                            
                            self.logger.info(
                                f"Processed {self.total_consumed:,}/{initial_queue_size:,} ({progress_percent:.1f}%) - "
                                f"Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} min"
                            )
                        
                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:
                            elapsed_time = current_time - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            remaining = initial_queue_size - self.total_consumed
                            eta_seconds = remaining / rate if rate > 0 else 0
                            eta_minutes = eta_seconds / 60
                            
                            self.logger.info(
                                f"PROGRESS: {self.total_consumed:,}/{initial_queue_size:,} records - "
                                f"Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} minutes - "
                                f"Failed: {self.total_failed}"
                            )
                            last_progress_report = current_time
                    else:
                        self.total_failed += 1
                        # Reject message without requeue
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        self.logger.error(f"Message rejected after all retries")
                
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    self.total_failed += 1
                    # Reject message without requeue
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Start consuming
            channel.basic_consume(
                queue="personal_data_queue",
                on_message_callback=process_message
            )
            
            self.logger.info("Consuming messages... Press Ctrl+C to stop")
            
            # Process messages until queue is empty
            while True:
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if queue is empty
                    remaining = self.get_queue_message_count(channel)
                    if remaining == 0:
                        self.logger.info("Queue is empty. All messages processed.")
                        break
                        
                except KeyboardInterrupt:
                    self.logger.info("Stopping consumer...")
                    break
                except Exception as e:
                    self.logger.error(f"Error during message processing: {e}")
                    # Try to reconnect
                    try:
                        connection.close()
                    except:
                        pass
                    time.sleep(5)
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=10)
                    channel.basic_consume(
                        queue="personal_data_queue",
                        on_message_callback=process_message
                    )
            
            # Close connection
            connection.close()
            
            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / initial_queue_size * 100) if initial_queue_size > 0 else 0
            
            self.logger.info(
                f"""
==========================================
Queue Consumer Summary:
==========================================
Initial queue size: {initial_queue_size:,}
Records consumed: {self.total_consumed:,}
Records failed: {self.total_failed}
Success rate: {success_rate:.1f}%
Total time: {total_time/60:.2f} minutes
Average rate: {avg_rate:.1f} records/second
==========================================
                """
            )
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            raise


def main():
    """Main function"""
    consumer = PersonalDataQueueConsumer()
    
    try:
        consumer.consume_queue()
    except KeyboardInterrupt:
        consumer.logger.info("Consumer stopped by user")
    except Exception as e:
        consumer.logger.error(f"Consumer failed: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
