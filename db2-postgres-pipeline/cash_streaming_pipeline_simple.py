#!/usr/bin/env python3
"""
Cash Information Streaming Pipeline - Simple and Fast
Following the agents pipeline pattern for better performance
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
from decimal import Decimal

from config import Config
from db2_connection import DB2Connection


@dataclass
class CashRecord:
    reportingDate: str
    branchCode: str
    cashCategory: str
    cashSubCategory: Optional[str]
    cashSubmissionTime: str
    currency: str
    cashDenomination: Optional[str]
    quantityOfCoinsNotes: Optional[int]
    orgAmount: Decimal
    usdAmount: Optional[Decimal]
    tzsAmount: Decimal
    transactionDate: str
    maturityDate: str
    allowanceProbableLoss: Decimal
    botProvision: Decimal


class CashStreamingPipeline:
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
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.logger.info("Cash Information STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")

    def get_cash_query(self, offset=0):
        """Get the cash information query with ROW_NUMBER pagination for DB2"""
        
        query = f"""
        SELECT * FROM (
            SELECT
                VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as reportingDate,
                gte.FK_UNITCODETRXUNIT AS branchCode,
                CASE
                    WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000010' OR gl.EXTERNAL_GLACCOUNT='101000015' THEN 'Cash in ATMs'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000004' OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Cash with Tellers'
                    ELSE 'unknown'
                END as cashCategory,
                CASE
                    WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'CleanNotes'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000002' OR
                         gl.EXTERNAL_GLACCOUNT='101000010' OR
                         gl.EXTERNAL_GLACCOUNT='101000004' OR
                         gl.EXTERNAL_GLACCOUNT='101000015' OR 
                         gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Notes'
                    ELSE NULL
                END as cashSubCategory,
                'Business Hours' as cashSubmissionTime,
                gte.CURRENCY_SHORT_DES as currency,
                CAST(NULL AS VARCHAR(50)) as cashDenomination,
                CAST(NULL AS INTEGER) as quantityOfCoinsNotes,
                gte.DC_AMOUNT AS orgAmount,
                CASE
                    WHEN gte.CURRENCY_SHORT_DES = 'USD'
                        THEN gte.DC_AMOUNT
                    ELSE NULL
                END AS usdAmount,
                CASE
                    WHEN gte.CURRENCY_SHORT_DES = 'USD'
                        THEN gte.DC_AMOUNT * 2500
                    ELSE
                        gte.DC_AMOUNT
                END AS tzsAmount,
                VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM') as transactionDate,
                VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as maturityDate,
                CAST(0 AS DECIMAL(18,2)) as allowanceProbableLoss,
                CAST(0 AS DECIMAL(18,2)) as botProvision,
                ROW_NUMBER() OVER (ORDER BY gte.TRN_DATE ASC, gte.FK_UNITCODETRXUNIT ASC) as rn
            FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
        ) AS numbered
        WHERE rn > {offset} AND rn <= {offset + self.batch_size}
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available cash records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT AS gte
        JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
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
        """Setup RabbitMQ queue for cash information"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue="cash_information_queue", durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'cash_information_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record"""
        try:
            return CashRecord(
                reportingDate=str(row[0]) if row[0] else None,
                branchCode=str(row[1]).strip() if row[1] else None,
                cashCategory=str(row[2]).strip() if row[2] else 'unknown',
                cashSubCategory=str(row[3]).strip() if row[3] else None,
                cashSubmissionTime=str(row[4]).strip() if row[4] else 'Business Hours',
                currency=str(row[5]).strip() if row[5] else None,
                cashDenomination=str(row[6]).strip() if row[6] else None,
                quantityOfCoinsNotes=int(row[7]) if row[7] else None,
                orgAmount=Decimal(str(row[8])) if row[8] else Decimal('0.00'),
                usdAmount=Decimal(str(row[9])) if row[9] else None,
                tzsAmount=Decimal(str(row[10])) if row[10] else Decimal('0.00'),
                transactionDate=str(row[11]) if row[11] else None,
                maturityDate=str(row[12]) if row[12] else None,
                allowanceProbableLoss=Decimal(str(row[13])) if row[13] else Decimal('0.00'),
                botProvision=Decimal(str(row[14])) if row[14] else Decimal('0.00'),
            )
        except Exception as e:
            self.logger.error(f"Error processing record: {e}")
            return None

    def insert_to_postgres(self, record: CashRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "cashInformation" (
            "reportingDate", "branchCode", "cashCategory", "cashSubCategory",
            "cashSubmissionTime", currency, "cashDenomination", "quantityOfCoinsNotes",
            "orgAmount", "usdAmount", "tzsAmount", "transactionDate",
            "maturityDate", "allowanceProbableLoss", "botProvision"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        cursor.execute(
            insert_sql,
            (
                record.reportingDate, record.branchCode, record.cashCategory, record.cashSubCategory,
                record.cashSubmissionTime, record.currency, record.cashDenomination, record.quantityOfCoinsNotes,
                record.orgAmount, record.usdAmount, record.tzsAmount, record.transactionDate,
                record.maturityDate, record.allowanceProbableLoss, record.botProvision
            ),
        )

    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")

            # Get total count
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]

            self.logger.info(f"Total cash records available: {self.total_available:,}")

            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()

            # Process batches
            batch_number = 1
            offset = 0
            last_progress_report = time.time()

            while offset < self.total_available:
                batch_start_time = time.time()

                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_cash_query(offset)
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
                                routing_key="cash_information_queue",
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
                offset += len(rows)
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
                    record = CashRecord(**record_data)

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

                        # Log every 100 records
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
            channel.basic_consume(queue="cash_information_queue", on_message_callback=process_message)

            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)

                    if self.producer_finished.is_set():
                        method = channel.queue_declare(
                            queue="cash_information_queue", durable=True, passive=True
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
                    channel.basic_consume(queue="cash_information_queue", on_message_callback=process_message)

            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()

        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()

    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Cash Information STREAMING pipeline...")

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
            Cash Information Pipeline Summary:
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

    parser = argparse.ArgumentParser(description="Cash Information Streaming Pipeline")
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Batch size for processing"
    )

    args = parser.parse_args()

    pipeline = CashStreamingPipeline(batch_size=args.batch_size)

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
