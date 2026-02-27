#!/usr/bin/env python3
"""
Cash Information Streaming Pipeline - Producer and Consumer run simultaneously
Resilient implementation with connection retry and error handling
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
        self.max_retries = 5
        self.retry_delay = 3

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.logger.info("Cash Information STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")

    def get_cash_query(self, last_cursor=None):
        """Get the cash information query with cursor-based pagination using composite key"""
        
        where_clause = """
        WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
        """
        
        if last_cursor:
            # Unpack cursor values
            unit_code, usr_code, line_num, trn_date, trn_snum = last_cursor
            where_clause += f"""
            AND (
                gte.TRN_DATE > TIMESTAMP('{trn_date}')
                OR (
                    gte.TRN_DATE = TIMESTAMP('{trn_date}')
                    AND (
                        gte.FK_UNITCODETRXUNIT > '{unit_code}'
                        OR (
                            gte.FK_UNITCODETRXUNIT = '{unit_code}'
                            AND (
                                gte.FK_USRCODE > '{usr_code}'
                                OR (
                                    gte.FK_USRCODE = '{usr_code}'
                                    AND (
                                        gte.LINE_NUM > {line_num}
                                        OR (
                                            gte.LINE_NUM = {line_num}
                                            AND gte.TRN_SNUM > {trn_snum}
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
            """
        
        query = f"""
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
            gte.FK_UNITCODETRXUNIT as cursor_unit_code,
            gte.FK_USRCODE as cursor_usr_code,
            gte.LINE_NUM as cursor_line_num,
            gte.TRN_DATE as cursor_trn_date,
            gte.TRN_SNUM as cursor_trn_snum
        FROM GLI_TRX_EXTRACT AS gte
        JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        {where_clause}
        ORDER BY gte.TRN_DATE ASC, gte.FK_UNITCODETRXUNIT ASC, gte.FK_USRCODE ASC, gte.LINE_NUM ASC, gte.TRN_SNUM ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
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

    def setup_rabbitmq_connection(self, max_retries=5):
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
        """Process a single record - returns None if record should be skipped"""
        # Remove cursor fields (last 5 fields: unit_code, usr_code, line_num, trn_date, trn_snum)
        row_data = row[:-5]
        
        try:
            return CashRecord(
                reportingDate=str(row_data[0]) if row_data[0] else None,
                branchCode=str(row_data[1]).strip() if row_data[1] else None,
                cashCategory=str(row_data[2]).strip() if row_data[2] else 'unknown',
                cashSubCategory=str(row_data[3]).strip() if row_data[3] else None,
                cashSubmissionTime=str(row_data[4]).strip() if row_data[4] else 'Business Hours',
                currency=str(row_data[5]).strip() if row_data[5] else None,
                cashDenomination=str(row_data[6]).strip() if row_data[6] else None,
                quantityOfCoinsNotes=int(row_data[7]) if row_data[7] else None,
                orgAmount=Decimal(str(row_data[8])) if row_data[8] else Decimal('0.00'),
                usdAmount=Decimal(str(row_data[9])) if row_data[9] else None,
                tzsAmount=Decimal(str(row_data[10])) if row_data[10] else Decimal('0.00'),
                transactionDate=str(row_data[11]) if row_data[11] else None,
                maturityDate=str(row_data[12]) if row_data[12] else None,
                allowanceProbableLoss=Decimal(str(row_data[13])) if row_data[13] else Decimal('0.00'),
                botProvision=Decimal(str(row_data[14])) if row_data[14] else Decimal('0.00'),
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

            # Get total count with retry
            for attempt in range(self.max_retries):
                try:
                    with self.db2_conn.get_connection(log_connection=True) as conn:
                        cursor = conn.cursor()
                        cursor.execute(self.get_total_count_query())
                        self.total_available = cursor.fetchone()[0]
                    break
                except Exception as e:
                    self.logger.warning(f"DB2 count query attempt {attempt + 1} failed: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * 2)  # Longer delay for connection issues
                    else:
                        raise

            self.logger.info(f"Total cash records available: {self.total_available:,}")

            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()

            # Process batches
            batch_number = 1
            last_cursor = None  # Will hold (unit_code, usr_code, line_num, trn_date, trn_snum)
            last_progress_report = time.time()

            while True:
                batch_start_time = time.time()

                # Fetch batch with retry and exponential backoff
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_cash_query(last_cursor)
                            cursor.execute(batch_query)
                            rows = cursor.fetchall()
                        break
                    except Exception as e:
                        wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        self.logger.warning(
                            f"DB2 query attempt {attempt + 1} failed: {e}. Waiting {wait_time}s before retry..."
                        )
                        if attempt < self.max_retries - 1:
                            time.sleep(wait_time)
                        else:
                            self.logger.error(f"Failed to fetch batch after {self.max_retries} attempts")
                            raise

                if not rows:
                    self.logger.info("No more records to process")
                    break

                # Process and publish
                batch_published = 0
                for row in rows:
                    # Extract cursor values (last 5 fields)
                    last_cursor = (
                        row[-5],  # FK_UNITCODETRXUNIT
                        row[-4],  # FK_USRCODE
                        row[-3],  # LINE_NUM
                        row[-2],  # TRN_DATE
                        row[-1],  # TRN_SNUM
                    )

                    record = self.process_record(row)

                    # Skip record if it's None (validation failed)
                    if record is None:
                        continue

                    message = json.dumps(asdict(record), default=str)

                    # Publish with retry
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
                time.sleep(0.1)

            connection.close()
            self.logger.info(
                f"Producer finished: {self.total_produced:,} records published"
            )
            self.producer_finished.set()

        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            import traceback
            traceback.print_exc()
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
                                self.logger.debug(f"Duplicate record skipped")
                                inserted = True
                                break
                            
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
                    self.logger.error(f"Consumer connection error: {e}")
                    try:
                        connection.close()
                    except:
                        pass
                    time.sleep(5)
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=10)
                    channel.basic_consume(queue="cash_information_queue", on_message_callback=process_message)

            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()

        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            import traceback
            traceback.print_exc()
            self.consumer_finished.set()

    def run_streaming_pipeline(self):
        """Run the streaming pipeline with producer and consumer threads"""
        try:
            self.logger.info("Setting up pipeline...")
            self.setup_rabbitmq_queue()

            # Start producer and consumer threads
            producer = threading.Thread(target=self.producer_thread, daemon=True)
            consumer = threading.Thread(target=self.consumer_thread, daemon=True)

            producer.start()
            time.sleep(2)  # Give producer a head start
            consumer.start()

            # Wait for both threads
            producer.join()
            consumer.join(timeout=60)

            if consumer.is_alive():
                self.stop_consumer.set()
                consumer.join(timeout=30)

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
        "--batch-size", type=int, default=1000, help="Batch size for processing"
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
