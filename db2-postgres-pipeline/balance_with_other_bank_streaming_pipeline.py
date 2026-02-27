#!/usr/bin/env python3
"""
Balance with Other Bank Streaming Pipeline - Producer and Consumer run simultaneously
Based on balance-with-other-bank-v1.sql
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
class BalanceWithOtherBankRecord:
    reportingDate: str
    accountNumber: str
    accountName: str
    bankCode: Optional[str]
    country: str
    relationshipType: str
    accountType: str
    subAccountType: Optional[str]
    currency: str
    orgAmount: Optional[Decimal]
    usdAmount: Optional[Decimal]
    tzsAmount: Optional[Decimal]
    transactionDate: str
    pastDueDays: Optional[int]
    allowanceProbableLoss: Decimal
    botProvision: Decimal
    assetsClassificationCategory: str
    contractDate: str
    maturityDate: str
    externalRatingCorrespondentBank: str
    gradesUnratedBanks: Optional[str]


class BalanceWithOtherBankStreamingPipeline:
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

        self.logger.info("Balance with Other Bank STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")

    def get_balance_query(self, last_account_number=None, last_trn_date=None):
        """Get the balance query with cursor-based pagination"""
        
        where_clause = "WHERE gl.EXTERNAL_GLACCOUNT IN ('100050001', '100013000', '100050000') AND pa.ACCOUNT_NUMBER <> ''"
        
        if last_account_number and last_trn_date:
            where_clause += f" AND (pa.ACCOUNT_NUMBER > '{last_account_number}' OR (pa.ACCOUNT_NUMBER = '{last_account_number}' AND gte.TRN_DATE > '{last_trn_date}'))"
        
        query = f"""
        SELECT CURRENT_TIMESTAMP                                  AS reportingDate,
               pa.ACCOUNT_NUMBER                                  as accountNumber,
               c.SURNAME                                          as accountName,
               CASE
                   WHEN UPPER(c.FIRST_NAME) = 'ECOBANK' THEN '040'
                   WHEN UPPER(c.FIRST_NAME) = 'BOA' THEN '009'
                   WHEN UPPER(c.FIRST_NAME) = 'TPB' THEN '048'
                   WHEN UPPER(c.FIRST_NAME) = 'TANZANIA POSTAL BANK' THEN '048'
                   END                                            AS bankCode,
               'TANZANIA, UNITED REPUBLIC OF'                     as Country,
               'Domestic bank related'                            as relationshipType,
               'Current'                                          as accountType,
               'Normal'                                               as subAccountType,
               gte.CURRENCY_SHORT_DES                             as currency,
               gte.DC_AMOUNT                                      AS orgAmount,

               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL(gte.DC_AMOUNT, 18, 2)

                   WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                       THEN DECIMAL(gte.DC_AMOUNT / fx.rate, 18, 2)

                   ELSE NULL
                   END                                            AS usdAmount,

               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL(gte.DC_AMOUNT * fx.rate, 18, 2)

                   ELSE DECIMAL(gte.DC_AMOUNT, 18, 2)
                   END                                            AS tzsAmount,
               gte.TRN_DATE                                       as transactionDate,
               (DATE(gte.AVAILABILITY_DATE) - DATE(gte.TRN_DATE)) AS pastDueDays,
               0                                                  as allowanceProbableLoss,
               0                                                  as botProvision,
               'Current'                                          as assetsClassificationCategory,
               gte.TRN_DATE                                       as contractDate,
               gte.AVAILABILITY_DATE                              as maturityDate,
               'Unrated'      as externalRatingCorrespondentBank,
               'Grade B'                                               as gradesUnratedBanks,
               pa.ACCOUNT_NUMBER                                  as cursor_account_number,
               gte.TRN_DATE                                       as cursor_trn_date

        FROM GLI_TRX_EXTRACT as gte

                 LEFT JOIN
             GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                 LEFT JOIN
             CUSTOMER c ON gte.CUST_ID = c.CUST_ID

                 LEFT JOIN
             PROFITS.PROFITS_ACCOUNT pa ON gte.CUST_ID = pa.CUST_ID and PRFT_SYSTEM = 3

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

        {where_clause}
        ORDER BY pa.ACCOUNT_NUMBER ASC, gte.TRN_DATE ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available balance records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT as gte
                 LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                 LEFT JOIN PROFITS.PROFITS_ACCOUNT pa ON gte.CUST_ID = pa.CUST_ID and PRFT_SYSTEM = 3
        WHERE gl.EXTERNAL_GLACCOUNT IN ('100050001', '100013000', '100050000')
          AND pa.ACCOUNT_NUMBER <> ''
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
        """Setup RabbitMQ queue for balance with other bank"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue="balance_with_other_bank_queue", durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'balance_with_other_bank_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record - returns None if record should be skipped"""
        # Remove cursor fields (last two)
        row_data = row[:-2]
        
        return BalanceWithOtherBankRecord(
            reportingDate=str(row_data[0]).strip() if row_data[0] else None,
            accountNumber=str(row_data[1]).strip() if row_data[1] else None,
            accountName=str(row_data[2]).strip() if row_data[2] else None,
            bankCode=str(row_data[3]).strip() if row_data[3] else None,
            country=str(row_data[4]).strip() if row_data[4] else 'TANZANIA, UNITED REPUBLIC OF',
            relationshipType=str(row_data[5]).strip() if row_data[5] else 'Domestic bank related',
            accountType=str(row_data[6]).strip() if row_data[6] else 'Current',
            subAccountType=str(row_data[7]).strip() if row_data[7] else None,
            currency=str(row_data[8]).strip() if row_data[8] else None,
            orgAmount=Decimal(str(row_data[9])) if row_data[9] is not None else None,
            usdAmount=Decimal(str(row_data[10])) if row_data[10] is not None else None,
            tzsAmount=Decimal(str(row_data[11])) if row_data[11] is not None else None,
            transactionDate=str(row_data[12]) if row_data[12] else None,
            pastDueDays=int(row_data[13]) if row_data[13] is not None else None,
            allowanceProbableLoss=Decimal(str(row_data[14])) if row_data[14] is not None else Decimal('0'),
            botProvision=Decimal(str(row_data[15])) if row_data[15] is not None else Decimal('0'),
            assetsClassificationCategory=str(row_data[16]).strip() if row_data[16] else 'Current',
            contractDate=str(row_data[17]) if row_data[17] else None,
            maturityDate=str(row_data[18]) if row_data[18] else None,
            externalRatingCorrespondentBank=str(row_data[19]).strip() if row_data[19] else 'Highly rated Multilateral Development Banks',
            gradesUnratedBanks=str(row_data[20]).strip() if row_data[20] else None,
        )

    def insert_to_postgres(self, record: BalanceWithOtherBankRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "balanceWithOtherBank" (
            "reportingDate", "accountNumber", "accountName", "bankCode",
            "country", "relationshipType", "accountType", "subAccountType",
            "currency", "orgAmount", "usdAmount", "tzsAmount",
            "transactionDate", "pastDueDays", "allowanceProbableLoss", "botProvision",
            "assetsClassificationCategory", "contractDate", "maturityDate",
            "externalRatingCorrespondentBank", "gradesUnratedBanks"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_sql,
            (
                record.reportingDate,
                record.accountNumber,
                record.accountName,
                record.bankCode,
                record.country,
                record.relationshipType,
                record.accountType,
                record.subAccountType,
                record.currency,
                record.orgAmount,
                record.usdAmount,
                record.tzsAmount,
                record.transactionDate,
                record.pastDueDays,
                record.allowanceProbableLoss,
                record.botProvision,
                record.assetsClassificationCategory,
                record.contractDate,
                record.maturityDate,
                record.externalRatingCorrespondentBank,
                record.gradesUnratedBanks,
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

            self.logger.info(f"Total balance records available: {self.total_available:,}")

            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()

            # Process batches
            batch_number = 1
            last_account_number = None
            last_trn_date = None
            last_progress_report = time.time()

            while True:
                batch_start_time = time.time()

                # Fetch batch (don't log individual connections)
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_balance_query(last_account_number, last_trn_date)
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
                    last_account_number = row[-2]  # cursor_account_number
                    last_trn_date = row[-1]  # cursor_trn_date

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
                                routing_key="balance_with_other_bank_queue",
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
                    
                    # Convert string decimals back to Decimal
                    if record_data.get('orgAmount'):
                        record_data['orgAmount'] = Decimal(record_data['orgAmount'])
                    if record_data.get('usdAmount'):
                        record_data['usdAmount'] = Decimal(record_data['usdAmount'])
                    if record_data.get('tzsAmount'):
                        record_data['tzsAmount'] = Decimal(record_data['tzsAmount'])
                    if record_data.get('allowanceProbableLoss'):
                        record_data['allowanceProbableLoss'] = Decimal(record_data['allowanceProbableLoss'])
                    if record_data.get('botProvision'):
                        record_data['botProvision'] = Decimal(record_data['botProvision'])
                    
                    record = BalanceWithOtherBankRecord(**record_data)

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
            channel.basic_consume(queue="balance_with_other_bank_queue", on_message_callback=process_message)

            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)

                    if self.producer_finished.is_set():
                        method = channel.queue_declare(
                            queue="balance_with_other_bank_queue", durable=True, passive=True
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
                        queue="balance_with_other_bank_queue", on_message_callback=process_message
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
        self.logger.info("Starting Balance with Other Bank STREAMING pipeline...")

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
            Balance with Other Bank Pipeline Summary:
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

    parser = argparse.ArgumentParser(description="Balance with Other Bank Streaming Pipeline")
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )

    args = parser.parse_args()

    pipeline = BalanceWithOtherBankStreamingPipeline(batch_size=args.batch_size)

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
