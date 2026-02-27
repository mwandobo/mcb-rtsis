#!/usr/bin/env python3
"""
Balances with MNOs Streaming Pipeline - Producer and Consumer run simultaneously
Based on balances-with-mnos.sql
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
class BalancesWithMnosRecord:
    reportingDate: str
    floatBalanceDate: str
    mnoCode: Optional[str]
    tillNumber: Optional[str]
    currency: str
    allowanceProbableLoss: Decimal
    botProvision: Decimal
    orgFloatAmount: Optional[Decimal]
    usdFloatAmount: Optional[Decimal]
    tzsFloatAmount: Optional[Decimal]


class BalancesWithMnosStreamingPipeline:
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

        self.logger.info("Balances with MNOs STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")

    def get_balances_query(self, last_unit_code=None, last_usr_code=None, last_line_num=None, last_trn_date=None, last_trn_snum=None):
        """Get the balances query with cursor-based pagination"""
        
        where_clause = "WHERE gl.EXTERNAL_GLACCOUNT IN ('144000058', '144000062')"
        
        if last_unit_code and last_usr_code and last_line_num and last_trn_date and last_trn_snum:
            where_clause += f""" AND (
                gte.FK_UNITCODETRXUNIT > '{last_unit_code}' OR
                (gte.FK_UNITCODETRXUNIT = '{last_unit_code}' AND gte.FK_USRCODE > '{last_usr_code}') OR
                (gte.FK_UNITCODETRXUNIT = '{last_unit_code}' AND gte.FK_USRCODE = '{last_usr_code}' AND gte.LINE_NUM > {last_line_num}) OR
                (gte.FK_UNITCODETRXUNIT = '{last_unit_code}' AND gte.FK_USRCODE = '{last_usr_code}' AND gte.LINE_NUM = {last_line_num} AND gte.TRN_DATE > '{last_trn_date}') OR
                (gte.FK_UNITCODETRXUNIT = '{last_unit_code}' AND gte.FK_USRCODE = '{last_usr_code}' AND gte.LINE_NUM = {last_line_num} AND gte.TRN_DATE = '{last_trn_date}' AND gte.TRN_SNUM > {last_trn_snum})
            )"""
        
        query = f"""
        SELECT
            VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYYHHMM') AS reportingDate,
            VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYYHHMM') AS floatBalanceDate,

            CASE gl.EXTERNAL_GLACCOUNT
                WHEN '144000058' THEN 'Tigo Pesa'
                WHEN '144000062' THEN 'M-Pesa'
            END AS mnoCode,

            CASE gl.EXTERNAL_GLACCOUNT
                WHEN '144000058' THEN '0710-338790'
                WHEN '144000062' THEN '711758'
            END AS tillNumber,

            gte.CURRENCY_SHORT_DES AS currency,
            0 AS allowanceProbableLoss,
            0 AS botProvision,

            gte.DC_AMOUNT                                      AS orgFloatAmount,

            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN DECIMAL(gte.DC_AMOUNT, 18, 2)

                WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                    THEN DECIMAL(gte.DC_AMOUNT / fx.rate, 18, 2)

                ELSE NULL
                END                                            AS usdFloatAmount,

            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN DECIMAL(gte.DC_AMOUNT * fx.rate, 18, 2)

                ELSE DECIMAL(gte.DC_AMOUNT, 18, 2)
                END                                            AS tzsFloatAmount,
            
            gte.FK_UNITCODETRXUNIT                             AS cursor_unit_code,
            gte.FK_USRCODE                                     AS cursor_usr_code,
            gte.LINE_NUM                                       AS cursor_line_num,
            gte.TRN_DATE                                       AS cursor_trn_date,
            gte.TRN_SNUM                                       AS cursor_trn_snum

        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN CURRENCY curr
                           ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

                 LEFT JOIN (SELECT fr.fk_currencyid_curr,
                                   fr.rate
                            FROM fixing_rate fr
                            WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                                  (SELECT fk_currencyid_curr,
                                          activation_date,
                                          MAX(activation_time)
                                   FROM fixing_rate
                                   WHERE activation_date = (SELECT MAX(b.activation_date)
                                                            FROM fixing_rate b
                                                            WHERE b.activation_date <= CURRENT_DATE)
                                   GROUP BY fk_currencyid_curr, activation_date)) fx
                           ON fx.fk_currencyid_curr = curr.ID_CURRENCY

                 JOIN GLG_ACCOUNT gl
            ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
        {where_clause}
        ORDER BY gte.FK_UNITCODETRXUNIT ASC, gte.FK_USRCODE ASC, gte.LINE_NUM ASC, gte.TRN_DATE ASC, gte.TRN_SNUM ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available balance records - disabled for performance"""
        # Note: Counting with complex JOINs is very slow
        # We'll skip the count and just process all records
        return None
    
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
        """Setup RabbitMQ queue for balances with MNOs"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue="balances_with_mnos_queue", durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'balances_with_mnos_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record - returns None if record should be skipped"""
        # Remove cursor fields (last five)
        row_data = row[:-5]
        
        return BalancesWithMnosRecord(
            reportingDate=str(row_data[0]).strip() if row_data[0] else None,
            floatBalanceDate=str(row_data[1]).strip() if row_data[1] else None,
            mnoCode=str(row_data[2]).strip() if row_data[2] else None,
            tillNumber=str(row_data[3]).strip() if row_data[3] else None,
            currency=str(row_data[4]).strip() if row_data[4] else None,
            allowanceProbableLoss=Decimal(str(row_data[5])) if row_data[5] is not None else Decimal('0'),
            botProvision=Decimal(str(row_data[6])) if row_data[6] is not None else Decimal('0'),
            orgFloatAmount=Decimal(str(row_data[7])) if row_data[7] is not None else None,
            usdFloatAmount=Decimal(str(row_data[8])) if row_data[8] is not None else None,
            tzsFloatAmount=Decimal(str(row_data[9])) if row_data[9] is not None else None,
        )

    def insert_to_postgres(self, record: BalancesWithMnosRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "balancesWithMnos" (
            "reportingDate", "floatBalanceDate", "mnoCode", "tillNumber",
            "currency", "allowanceProbableLoss", "botProvision",
            "orgFloatAmount", "usdFloatAmount", "tzsFloatAmount"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_sql,
            (
                record.reportingDate,
                record.floatBalanceDate,
                record.mnoCode,
                record.tillNumber,
                record.currency,
                record.allowanceProbableLoss,
                record.botProvision,
                record.orgFloatAmount,
                record.usdFloatAmount,
                record.tzsFloatAmount,
            ),
        )

    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")

            # Skip total count for performance (DISTINCT count is very slow)
            self.total_available = 0  # Unknown
            self.logger.info("Total count skipped (processing all available records)")

            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()

            # Process batches
            batch_number = 1
            last_unit_code = None
            last_usr_code = None
            last_line_num = None
            last_trn_date = None
            last_trn_snum = None
            last_progress_report = time.time()

            while True:
                batch_start_time = time.time()

                # Fetch batch (don't log individual connections)
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_balances_query(last_unit_code, last_usr_code, last_line_num, last_trn_date, last_trn_snum)
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
                    last_unit_code = row[-5]  # cursor_unit_code
                    last_usr_code = row[-4]  # cursor_usr_code
                    last_line_num = row[-3]  # cursor_line_num
                    last_trn_date = row[-2]  # cursor_trn_date
                    last_trn_snum = row[-1]  # cursor_trn_snum

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
                                routing_key="balances_with_mnos_queue",
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

                self.logger.info(
                    f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published (Total: {self.total_produced:,}, {batch_time:.1f}s)"
                )

                # Progress report every 5 minutes
                current_time = time.time()
                if current_time - last_progress_report >= 300:
                    elapsed_time = current_time - self.start_time
                    rate = (
                        self.total_produced / elapsed_time if elapsed_time > 0 else 0
                    )

                    self.logger.info(
                        f"PROGRESS REPORT: {self.total_produced:,} records produced - Rate: {rate:.1f} rec/sec - Time: {elapsed_time/60:.1f} min"
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
                    
                    # Convert string decimals back to Decimal
                    if record_data.get('allowanceProbableLoss'):
                        record_data['allowanceProbableLoss'] = Decimal(record_data['allowanceProbableLoss'])
                    if record_data.get('botProvision'):
                        record_data['botProvision'] = Decimal(record_data['botProvision'])
                    if record_data.get('orgFloatAmount'):
                        record_data['orgFloatAmount'] = Decimal(record_data['orgFloatAmount'])
                    if record_data.get('usdFloatAmount'):
                        record_data['usdFloatAmount'] = Decimal(record_data['usdFloatAmount'])
                    if record_data.get('tzsFloatAmount'):
                        record_data['tzsFloatAmount'] = Decimal(record_data['tzsFloatAmount'])
                    
                    record = BalancesWithMnosRecord(**record_data)

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
                            self.logger.info(
                                f"Consumer: Processed {self.total_consumed:,} records - Rate: {rate:.1f} rec/sec"
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

                            self.logger.info(
                                f"CONSUMER PROGRESS: {self.total_consumed:,} records - Rate: {rate:.1f} rec/sec - Time: {elapsed_time/60:.1f} min"
                            )
                            last_progress_report = current_time

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue="balances_with_mnos_queue", on_message_callback=process_message)

            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)

                    if self.producer_finished.is_set():
                        method = channel.queue_declare(
                            queue="balances_with_mnos_queue", durable=True, passive=True
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
                        queue="balances_with_mnos_queue", on_message_callback=process_message
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
        self.logger.info("Starting Balances with MNOs STREAMING pipeline...")

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
            Balances with MNOs Pipeline Summary:
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

    parser = argparse.ArgumentParser(description="Balances with MNOs Streaming Pipeline")
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )

    args = parser.parse_args()

    pipeline = BalancesWithMnosStreamingPipeline(batch_size=args.batch_size)

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
