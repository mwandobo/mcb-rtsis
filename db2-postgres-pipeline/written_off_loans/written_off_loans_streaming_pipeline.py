#!/usr/bin/env python3
"""
Written Off Loans Streaming Pipeline - Producer and Consumer run simultaneously
Based on written-off-loans.sql query

Anchor: LOAN_ACCOUNT WHERE LOAN_STATUS = '4' (write off), enriched with
LOAN_TRX_RECORDING (write-off event, full-history interest rate, earliest
transaction officer) to recover fields that get reset on LOAN_ACCOUNT
itself by the write-off batch procedure. See written-off-loans.sql for the
full reasoning behind each field mapping.
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

# NOTE: logging is intentionally NOT configured here at module level.
# logging.basicConfig() only takes effect on its first call - if this ran at
# import time, it would win the race against run_written_off_loans_pipeline.py's
# own basicConfig(handlers=[FileHandler(...), StreamHandler()]) call, since
# that script imports this module before calling its own setup. The result
# would be silent: log output still appears (via the default StreamHandler
# a bare basicConfig installs), but the log FILE stays empty because the
# real handler config never took effect. Logging setup for direct
# invocation lives in this module's own __main__ guard instead.


@dataclass
class WrittenOffLoanRecord:
    """Data class for written off loan records based on written-off-loans.sql"""
    reportingDate: str
    customerIdentificationNumber: str
    accountNumber: str
    borrowerName: str
    borrowerCountry: str
    clientType: str
    loanNumber: str
    loanType: str
    currency: str
    orgDisbursedAmount: str
    usdDisbursedAmount: str
    tzsDisbursedAmount: str
    disbursementDate: str
    gracePeriodPaymentPrincipal: str
    maturityDate: str
    orgGrossPaidAmount: str
    usdGrossPaidAmount: str
    tzsGrossPaidAmount: str
    writtenOffDate: str
    orgOutstandingPrincipalAmount: str
    usdOutstandingPrincipalAmount: str
    tzsOutstandingPrincipalAmount: str
    annualInterestRate: str
    latestInstallmentPayDate: Optional[str]
    collateralPledged: str
    orgCollateralValue: str
    usdCollateralValue: str
    tzsCollateralValue: str
    pastDueDays: str
    loanOfficer: str
    loanSupervisor: str


class WrittenOffLoansStreamingPipeline:
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

        self.logger.info("Written Off Loans STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")

    def get_written_off_loans_query(self):
        """Get the written off loans query from written-off-loans.sql"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'written-off-loans.sql'
        )

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            return f.read()

    def get_total_count(self):
        """Get total count of written off loan accounts from DB2"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM LOAN_ACCOUNT WHERE LOAN_STATUS = '4'
                """)
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Written off loan accounts (LOAN_STATUS='4'): {count:,}")
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
        """Setup RabbitMQ queue for written off loans with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()

            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='written_off_loans_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='written_off_loans_dead_letter', durable=True)
            channel.queue_bind(
                queue='written_off_loans_dead_letter',
                exchange='written_off_loans_dlx',
                routing_key='written_off_loans_queue'
            )

            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='written_off_loans_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'written_off_loans_dlx',
                        'x-dead-letter-routing-key': 'written_off_loans_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments
                self.logger.warning(
                    "Queue 'written_off_loans_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='written_off_loans_queue', durable=True)
                self.logger.info("RabbitMQ queue 'written_off_loans_queue' setup complete (without DLX)")

            connection.close()

        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single written off loan record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()

            # Map the fields from written-off-loans.sql to the dataclass
            record = WrittenOffLoanRecord(
                reportingDate=safe_string(row[0]),                                # VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')
                customerIdentificationNumber=safe_string(row[1]),                 # cust.CUST_ID
                accountNumber=safe_string(row[2]),                                # pa.ACCOUNT_NUMBER
                borrowerName=safe_string(row[3]),                                 # CUSTOMER first+middle+surname
                borrowerCountry=safe_string(row[4]),                              # COUNTRIES_LOOKUP / TZ fallback
                clientType=safe_string(row[5]),                                   # CUSTOMER_TYPES_LOOKUP
                loanNumber=safe_string(row[6]),                                   # pa.ACCOUNT_NUMBER
                loanType=safe_string(row[7]),                                     # PRODUCT.DESCRIPTION derived
                currency=safe_string(row[8]),                                     # CURRENCY.SHORT_DESCR
                orgDisbursedAmount=safe_string(row[9]),                           # LOAN_ACCOUNT.TOT_DRAWDOWN_AMN
                usdDisbursedAmount=safe_string(row[10]),
                tzsDisbursedAmount=safe_string(row[11]),
                disbursementDate=safe_string(row[12]),                            # DRAWDOWN_FST_DT / ACC_OPEN_DT
                gracePeriodPaymentPrincipal=safe_string(row[13]),                 # INSTALL_FIRST_DT - disbursement
                maturityDate=safe_string(row[14]),                                # ACC_EXP_DT
                orgGrossPaidAmount=safe_string(row[15]),                          # disbursed - written off + interest paid
                usdGrossPaidAmount=safe_string(row[16]),
                tzsGrossPaidAmount=safe_string(row[17]),
                writtenOffDate=safe_string(row[18]),                              # LOAN_TRX_RECORDING TRX_CODE 74211
                orgOutstandingPrincipalAmount=safe_string(row[19]),               # LOAN_TRX_RECORDING TRX_CODE 74211
                usdOutstandingPrincipalAmount=safe_string(row[20]),
                tzsOutstandingPrincipalAmount=safe_string(row[21]),
                annualInterestRate=safe_string(row[22]),                         # MAX(rate) over full trx history
                latestInstallmentPayDate=safe_string(row[23]) if row[23] else None,  # not mandatory
                collateralPledged=safe_string(row[24]),                          # ACCOUNT_COLLATERAL / 'None'
                orgCollateralValue=safe_string(row[25]),
                usdCollateralValue=safe_string(row[26]),
                tzsCollateralValue=safe_string(row[27]),
                pastDueDays=safe_string(row[28]),                                # LOAN_INSTALLMENTS.RQ_OVERDUE_DAYS
                loanOfficer=safe_string(row[29]),                                # earliest txn TRX_USR -> BANKEMPLOYEE
                loanSupervisor=safe_string(row[30])                              # same resolved officer
            )

            return record

        except Exception as e:
            self.logger.error(f"Error processing written off loan record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise

    def validate_record(self, record):
        """Validate written off loan record"""
        try:
            # Basic validation
            if not record.loanNumber:
                self.logger.warning("Missing loan number")
                return False

            if not record.customerIdentificationNumber:
                self.logger.warning("Missing customer identification number")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating written off loan record: {e}")
            return False

    def producer_thread(self):
        """Producer thread - executes query ONCE and streams results via fetchmany()"""
        try:
            self.logger.info("Producer thread started")

            # Get dynamic record count
            self.total_available = self.get_total_count()

            self.logger.info(f"Total written off loan accounts available: {self.total_available:,}")

            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()

            # Execute the query ONCE and stream results
            query = self.get_written_off_loans_query()
            self.logger.info("Executing written off loans query (single execution, streaming results)...")

            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                db2_cursor = db2_conn.cursor()
                db2_cursor.execute(query)
                self.logger.info("Query executed successfully, streaming results via fetchmany()...")

                batch_number = 1

                while True:
                    batch_start_time = time.time()

                    # Fetch next chunk from the already-running query
                    rows = db2_cursor.fetchmany(self.batch_size)

                    if not rows:
                        self.logger.info("No more records to fetch")
                        break

                    # Process and publish
                    batch_published = 0
                    for row in rows:
                        record = self.process_record(row)

                        if self.validate_record(record):
                            message = json.dumps(asdict(record), default=str)

                            # Publish with retry
                            published = False
                            for attempt in range(self.max_retries):
                                try:
                                    channel.basic_publish(
                                        exchange='',
                                        routing_key='written_off_loans_queue',
                                        body=message,
                                        properties=pika.BasicProperties(delivery_mode=2)
                                    )
                                    published = True
                                    break
                                except Exception as e:
                                    self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                                    if attempt < self.max_retries - 1:
                                        try:
                                            rmq_connection.close()
                                        except Exception:
                                            pass
                                        rmq_connection, channel = self.setup_rabbitmq_connection()
                                        time.sleep(self.retry_delay)
                                    else:
                                        self.logger.error(f"Failed to publish message after {self.max_retries} attempts")

                            if published:
                                batch_published += 1
                                with self._stats_lock:
                                    self.total_produced += 1

                    batch_time = time.time() - batch_start_time
                    with self._stats_lock:
                        produced = self.total_produced
                    progress_percent = produced / self.total_available * 100 if self.total_available > 0 else 0

                    self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} rows fetched, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")

                    batch_number += 1

            rmq_connection.close()
            with self._stats_lock:
                produced = self.total_produced
            self.logger.info(f"Producer finished: {produced:,} records published out of {self.total_available:,} available")
            self.producer_finished.set()

        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()

    def consumer_thread(self):
        """Consumer thread - processes messages from queue with batch inserts and persistent connection"""
        pg_conn = None
        try:
            self.logger.info("Consumer thread started")

            # Setup persistent PostgreSQL connection
            pg_conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            self.logger.info("Consumer: Persistent PostgreSQL connection established")

            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()

            # Batch insert buffer
            insert_buffer: List[WrittenOffLoanRecord] = []
            pending_tags: List[int] = []
            last_flush_time = time.time()
            flush_interval = 5  # seconds

            def flush_buffer(ch):
                """Flush buffered records to PostgreSQL in a single batch insert"""
                nonlocal insert_buffer, pending_tags, last_flush_time, pg_conn
                if not insert_buffer:
                    return

                batch_size = len(insert_buffer)
                try:
                    self.insert_batch_to_postgres(insert_buffer, pg_conn)

                    # Batch acknowledge all messages at once
                    if pending_tags:
                        ch.basic_ack(delivery_tag=pending_tags[-1], multiple=True)

                    with self._stats_lock:
                        self.total_consumed += batch_size

                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()

                except psycopg2.OperationalError as e:
                    self.logger.error(f"PostgreSQL connection lost during batch insert: {e}")
                    # Reconnect PostgreSQL
                    try:
                        pg_conn.close()
                    except Exception:
                        pass
                    pg_conn = psycopg2.connect(
                        host=self.config.database.pg_host,
                        port=self.config.database.pg_port,
                        database=self.config.database.pg_database,
                        user=self.config.database.pg_user,
                        password=self.config.database.pg_password
                    )
                    # Nack all to requeue for retry
                    for tag in pending_tags:
                        try:
                            ch.basic_nack(delivery_tag=tag, requeue=True)
                        except Exception:
                            pass
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()

                except Exception as e:
                    self.logger.error(f"Batch insert failed: {e}")
                    try:
                        pg_conn.rollback()
                    except Exception:
                        pass
                    # Nack all messages - they go to dead-letter queue
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
                    record = WrittenOffLoanRecord(**record_data)

                    insert_buffer.append(record)
                    pending_tags.append(method.delivery_tag)

                    # Flush if buffer is full or time interval exceeded
                    if len(insert_buffer) >= self.consumer_batch_size or \
                       time.time() - last_flush_time >= flush_interval:
                        flush_buffer(ch)

                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            # Set QoS to match consumer batch size for efficient batching
            channel.basic_qos(prefetch_count=self.consumer_batch_size)
            channel.basic_consume(queue='written_off_loans_queue', on_message_callback=process_message)

            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)

                    # Flush any remaining buffered records on timeout
                    if insert_buffer and time.time() - last_flush_time >= flush_interval:
                        flush_buffer(channel)

                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Flush remaining buffer before checking queue
                        flush_buffer(channel)

                        # Producer is done, check if queue is empty. A single
                        # passive declare right after producer_finished can
                        # race ahead of straggler deliveries still working
                        # their way through this consumer's prefetch window
                        # (observed losing messages on small, fast runs where
                        # publish-to-drain happens in well under a second) -
                        # require the queue to report empty on two checks in
                        # a row, pumping events and flushing between them, so
                        # any in-flight deliveries get a chance to land first.
                        queue_state = channel.queue_declare(queue='written_off_loans_queue', durable=True, passive=True)
                        if queue_state.method.message_count == 0:
                            connection.process_data_events(time_limit=1)
                            flush_buffer(channel)
                            queue_state = channel.queue_declare(queue='written_off_loans_queue', durable=True, passive=True)
                            if queue_state.method.message_count == 0 and not insert_buffer:
                                self.logger.info("Consumer: Queue empty, producer finished")
                                break

                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect RabbitMQ
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='written_off_loans_queue', on_message_callback=process_message)

            connection.close()
            with self._stats_lock:
                consumed = self.total_consumed
            self.logger.info(f"Consumer finished: {consumed:,} records processed")
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

    def insert_batch_to_postgres(self, records: List[WrittenOffLoanRecord], pg_conn):
        """Batch insert written off loan records to PostgreSQL with duplicate prevention"""
        try:
            cursor = pg_conn.cursor()

            insert_query = """
            INSERT INTO "writtenOffLoans" (
                "reportingDate", "customerIdentificationNumber", "accountNumber", "borrowerName",
                "borrowerCountry", "clientType", "loanNumber", "loanType", "currency",
                "orgDisbursedAmount", "usdDisbursedAmount", "tzsDisbursedAmount", "disbursementDate",
                "gracePeriodPaymentPrincipal", "maturityDate", "orgGrossPaidAmount", "usdGrossPaidAmount",
                "tzsGrossPaidAmount", "writtenOffDate", "orgOutstandingPrincipalAmount",
                "usdOutstandingPrincipalAmount", "tzsOutstandingPrincipalAmount", "annualInterestRate",
                "latestInstallmentPayDate", "collateralPledged", "orgCollateralValue", "usdCollateralValue",
                "tzsCollateralValue", "pastDueDays", "loanOfficer", "loanSupervisor"
            ) VALUES %s
            ON CONFLICT ("loanNumber") DO NOTHING
            """

            values = [
                (
                    r.reportingDate, r.customerIdentificationNumber, r.accountNumber, r.borrowerName,
                    r.borrowerCountry, r.clientType, r.loanNumber, r.loanType, r.currency,
                    r.orgDisbursedAmount, r.usdDisbursedAmount, r.tzsDisbursedAmount, r.disbursementDate,
                    r.gracePeriodPaymentPrincipal, r.maturityDate, r.orgGrossPaidAmount, r.usdGrossPaidAmount,
                    r.tzsGrossPaidAmount, r.writtenOffDate, r.orgOutstandingPrincipalAmount,
                    r.usdOutstandingPrincipalAmount, r.tzsOutstandingPrincipalAmount, r.annualInterestRate,
                    r.latestInstallmentPayDate, r.collateralPledged, r.orgCollateralValue, r.usdCollateralValue,
                    r.tzsCollateralValue, r.pastDueDays, r.loanOfficer, r.loanSupervisor
                )
                for r in records
            ]

            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()

        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} written off loan records: {e}")
            raise

    def ensure_unique_index(self):
        """Ensure unique index on loanNumber exists for ON CONFLICT duplicate prevention"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_writtenoffloans_loan_number_unique
                    ON "writtenOffLoans" ("loanNumber")
                """)
                conn.commit()
                self.logger.info("Unique index on loanNumber verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on loanNumber: {e}")
            raise

    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Written Off Loans STREAMING pipeline...")

        try:
            # Update state to running
            self._update_state('running')

            # Ensure unique index for duplicate prevention
            self.ensure_unique_index()

            # Setup queue
            self.setup_rabbitmq_queue()

            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()

            # Small delay to ensure consumer is ready
            time.sleep(1)

            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()

            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("Producer thread completed")

            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=60)  # Wait up to 60 seconds

            if consumer_thread.is_alive():
                self.logger.info("Stopping consumer thread...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)

            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0

            self.logger.info(f"""
            ==========================================
            Written Off Loans Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time:.1f} seconds
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)

            # Update pipeline state
            self._update_state('completed')

        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            self._update_state('failed', str(e))
            raise

    def _update_state(self, status, error_message=None):
        """Update pipeline state in the state table"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            state_manager.update_run(
                'written_off_loans',
                status,
                self.total_consumed,
                error_message
            )
            self.logger.info(f"State updated: {status}, records: {self.total_consumed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Written Off Loans Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for DB2 query pagination')
    parser.add_argument('--consumer-batch-size', type=int, default=100, help='Batch size for PostgreSQL inserts')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')

    args = parser.parse_args()

    # Create pipeline
    pipeline = WrittenOffLoansStreamingPipeline(batch_size=args.batch_size, consumer_batch_size=args.consumer_batch_size)

    try:
        if args.mode == 'producer':
            pipeline.producer_thread()
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:  # streaming
            pipeline.run_streaming_pipeline()

    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    # Only configure logging here (direct invocation) - see the note at the
    # top of this file on why it must not happen at module import time.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
