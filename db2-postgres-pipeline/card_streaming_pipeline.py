#!/usr/bin/env python3
"""
Card Streaming Pipeline - Producer and Consumer run simultaneously
Based on card_information.sql
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
class CardRecord:
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


class CardStreamingPipeline:
    def __init__(self, batch_size=1000):
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
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.logger.info("Card STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")

    def get_card_query(self, last_card_number=None):
        """Get the card query with cursor-based pagination"""
        
        where_clause = ""
        
        if last_card_number:
            where_clause = f"WHERE CA.FULL_CARD_NO > '{last_card_number}'"
        
        query = f"""
        SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYYHHMM') AS reportingDate,
               'MWCOTZTZ' AS bankCode,
               CA.FULL_CARD_NO AS cardNumber,
               RIGHT(TRIM(CA.FULL_CARD_NO), 10) AS binNumber,
               CA.FK_CUST_ID AS customerIdentificationNumber,
               'Debit' AS cardType,
               NULL AS cardTypeSubCategory,
               VARCHAR_FORMAT(CA.TUN_DATE,'DDMMYYYYHHMM') AS cardIssueDate,
               'Mwalimu Commercial Bank Plc' AS cardIssuer,
               'Domestic' AS cardIssuerCategory,
               'TANZANIA, UNITED REPUBLIC OF' AS cardIssuerCountry,
               CA.CARD_NAME_LATIN AS cardHolderName,
               CASE
                   WHEN CURRENT_DATE > CA.CARD_EXPIRY_DATE then 'Active'
                   ELSE 'Inactive'
               END AS cardStatus,
               'VISA' AS cardScheme,
               'UBX Tanzania Limited' AS acquiringPartner,
               VARCHAR_FORMAT(CA.CARD_EXPIRY_DATE,'DDMMYYYYHHMM') AS cardExpireDate,
               CA.FULL_CARD_NO AS cursor_card_number
        FROM CMS_CARD CA
        {where_clause}
        ORDER BY CA.FULL_CARD_NO ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available card records"""
        return """
        SELECT COUNT(*) as total_count
        FROM CMS_CARD
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
                password=self.config.database.pg_password,
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
                return connection, channel

            except Exception as e:
                self.logger.warning(
                    f"RabbitMQ connection attempt {attempt + 1} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for cards"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue="card_queue", durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'card_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record - returns None if record should be skipped"""
        # Remove cursor field (last one)
        row_data = row[:-1]
        
        return CardRecord(
            reportingDate=str(row_data[0]).strip() if row_data[0] else None,
            bankCode=str(row_data[1]).strip() if row_data[1] else 'MWCOTZTZ',
            cardNumber=str(row_data[2]).strip() if row_data[2] else None,
            binNumber=str(row_data[3]).strip() if row_data[3] else None,
            customerIdentificationNumber=str(row_data[4]).strip() if row_data[4] else None,
            cardType=str(row_data[5]).strip() if row_data[5] else 'Debit',
            cardTypeSubCategory=str(row_data[6]).strip() if row_data[6] else None,
            cardIssueDate=str(row_data[7]).strip() if row_data[7] else None,
            cardIssuer=str(row_data[8]).strip() if row_data[8] else 'Mwalimu Commercial Bank Plc',
            cardIssuerCategory=str(row_data[9]).strip() if row_data[9] else 'Domestic',
            cardIssuerCountry=str(row_data[10]).strip() if row_data[10] else 'TANZANIA, UNITED REPUBLIC OF',
            cardHolderName=str(row_data[11]).strip() if row_data[11] else None,
            cardStatus=str(row_data[12]).strip() if row_data[12] else 'Active',
            cardScheme=str(row_data[13]).strip() if row_data[13] else 'VISA',
            acquiringPartner=str(row_data[14]).strip() if row_data[14] else 'UBX Tanzania Limited',
            cardExpireDate=str(row_data[15]).strip() if row_data[15] else None,
        )

    def insert_to_postgres(self, record: CardRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "cardInformation" (
            "reportingDate", "bankCode", "cardNumber", "binNumber",
            "customerIdentificationNumber", "cardType", "cardTypeSubCategory",
            "cardIssueDate", "cardIssuer", "cardIssuerCategory", "cardIssuerCountry",
            "cardHolderName", "cardStatus", "cardScheme", "acquiringPartner",
            "cardExpireDate"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_sql,
            (
                record.reportingDate,
                record.bankCode,
                record.cardNumber,
                record.binNumber,
                record.customerIdentificationNumber,
                record.cardType,
                record.cardTypeSubCategory,
                record.cardIssueDate,
                record.cardIssuer,
                record.cardIssuerCategory,
                record.cardIssuerCountry,
                record.cardHolderName,
                record.cardStatus,
                record.cardScheme,
                record.acquiringPartner,
                record.cardExpireDate,
            ),
        )

    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")

            # Get total count (log this connection)
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]

            self.logger.info(f"Total card records available: {self.total_available:,}")

            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()

            # Process batches
            batch_number = 1
            last_card_number = None
            last_progress_report = time.time()

            while True:
                batch_start_time = time.time()

                # Fetch batch (don't log individual connections)
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_card_query(last_card_number)
                            cursor.execute(batch_query)
                            rows = cursor.fetchall()
                        break
                    except Exception as e:
                        self.logger.warning(
                            f"DB2 query attempt {attempt + 1} failed: {e}"
                        )
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
                    last_card_number = row[-1]  # cursor_card_number

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
                                exchange="",
                                routing_key="card_queue",
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2),
                            )
                            published = True
                            break
                        except Exception as e:
                            self.logger.warning(
                                f"RabbitMQ publish attempt {attempt + 1} failed: {e}"
                            )
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
                progress_percent = (
                    self.total_produced / self.total_available * 100
                    if self.total_available > 0
                    else 0
                )

                self.logger.info(
                    f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)"
                )

                # Progress report every 5 minutes
                current_time = time.time()
                if current_time - last_progress_report >= 300:
                    elapsed_time = current_time - self.start_time
                    rate = (
                        self.total_produced / elapsed_time if elapsed_time > 0 else 0
                    )
                    remaining_records = self.total_available - self.total_produced
                    eta_seconds = remaining_records / rate if rate > 0 else 0
                    eta_minutes = eta_seconds / 60

                    self.logger.info(
                        f"PROGRESS REPORT: {self.total_produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} minutes"
                    )
                    last_progress_report = current_time

                batch_number += 1
                time.sleep(0.1)

            connection.close()
            self.logger.info(
                f"Producer finished: {self.total_produced:,} records published"
            )
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
                    record = CardRecord(**record_data)

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
                            self.logger.warning(
                                f"PostgreSQL insert attempt {attempt + 1} failed: {e}"
                            )
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)

                    if inserted:
                        self.total_consumed += 1

                        # Log more frequently to show concurrent activity
                        if self.total_consumed % 100 == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = (
                                self.total_consumed / elapsed_time
                                if elapsed_time > 0
                                else 0
                            )
                            progress_percent = (
                                (self.total_consumed / self.total_available * 100)
                                if self.total_available > 0
                                else 0
                            )
                            self.logger.info(
                                f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}%) - Rate: {rate:.1f} rec/sec"
                            )

                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:
                            elapsed_time = current_time - self.start_time
                            rate = (
                                self.total_consumed / elapsed_time
                                if elapsed_time > 0
                                else 0
                            )
                            remaining_records = (
                                self.total_available - self.total_consumed
                                if self.total_available > 0
                                else 0
                            )
                            eta_seconds = remaining_records / rate if rate > 0 else 0
                            eta_minutes = eta_seconds / 60

                            self.logger.info(
                                f"CONSUMER PROGRESS: {self.total_consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} minutes"
                            )
                            last_progress_report = current_time

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue="card_queue", on_message_callback=process_message)

            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)

                    if self.producer_finished.is_set():
                        method = channel.queue_declare(
                            queue="card_queue", durable=True, passive=True
                        )
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
                    channel.basic_consume(
                        queue="card_queue", on_message_callback=process_message
                    )

            connection.close()
            self.logger.info(
                f"Consumer finished: {self.total_consumed:,} records processed"
            )
            self.consumer_finished.set()

        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()

    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Card STREAMING pipeline...")

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
            success_rate = (
                (self.total_consumed / self.total_produced * 100)
                if self.total_produced > 0
                else 0
            )

            self.logger.info(
                f"""
            ==========================================
            Card Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/60:.2f} minutes
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """
            )

        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description="Card Streaming Pipeline")
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )

    args = parser.parse_args()

    pipeline = CardStreamingPipeline(batch_size=args.batch_size)

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
