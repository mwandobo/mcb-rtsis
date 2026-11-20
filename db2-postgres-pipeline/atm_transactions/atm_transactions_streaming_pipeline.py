#!/usr/bin/env python3
"""
ATM Transactions Streaming Pipeline - Producer and Consumer run simultaneously
Uses atm-transaction-v1.sql query for comprehensive ATM transaction data extraction
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
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection


@dataclass
class AtmTransactionRecord:
    """Data class for ATM transaction records based on atm-transaction-v1.sql"""
    reportingDate: str
    atmCode: str
    transactionDate: str
    transactionId: str
    transactionType: Optional[str]
    currency: str
    orgTransactionAmount: Optional[float]
    tzsTransactionAmount: Optional[float]
    atmChannel: str
    valueAddedTaxAmount: Optional[float]
    exciseDutyAmount: Optional[float]
    electronicLevyAmount: Optional[float]


class AtmTransactionsStreamingPipeline:
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
        self.retry_delay = 5  # seconds
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("ATM Transactions STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_atm_transactions_query(self, last_timestamp=None):
        """Get the ATM transactions query using atm-transaction-v1.sql with optional TMSTAMP filter"""
        
        # Read the atm-transaction-v1.sql file
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'sqls', 'atm-transaction-v1.sql')
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read().strip()
        
        # For incremental mode, add TMSTAMP filter on ATM_TRX_RECORDING table
        if last_timestamp:
            if last_timestamp == 'epoch':
                timestamp_value = '1900-01-01-00.00.00.000000'
            else:
                timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]
            
            # Check if SQL already has WHERE clause
            sql_upper = sql.upper()
            if 'WHERE' in sql_upper:
                sql = sql + f"\n AND atx.TMSTAMP > '{timestamp_value}'"
            else:
                sql = sql + f"\n WHERE atx.TMSTAMP > '{timestamp_value}'"
        
        return sql
    
    def get_last_successful_run(self):
        """Get last successful run timestamp from state manager"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            return state_manager.get_last_successful_run('atm_transactions')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None
    
    def get_total_count_query(self):
        """Get total count of available ATM transaction records"""
        return """
        SELECT COUNT(*) as total_count
        FROM ATM_TRX_RECORDING atx
        LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = atx.PROCESSING_CODE 
        WHERE TERMINAL in('MWL01001','MWL01002')
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
        """Setup RabbitMQ queue for ATM transactions"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare queue with durability
            channel.queue_declare(queue='atm_transactions_queue', durable=True)
            
            connection.close()
            self.logger.info("RabbitMQ queue 'atm_transactions_queue' setup complete")
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single ATM transaction record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                return str(value).strip()
            
            def safe_float(value):
                """Safely convert to float"""
                if value is None:
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            # Map the fields from the SQL query to the dataclass
            # Based on the exact field order in atm-transaction-v1.sql:
            # reportingDate, atmCode, transactionDate, transactionId, transactionType,
            # currency, orgTransactionAmount, tzsTransactionAmount, atmChannel,
            # valueAddedTaxAmount, exciseDutyAmount, electronicLevyAmount
            record = AtmTransactionRecord(
                reportingDate=safe_string(row[0]),           # VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')
                atmCode=safe_string(row[1]),                 # atx.TERMINAL
                transactionDate=safe_string(row[2]),         # VARCHAR_FORMAT(atx.TUN_DATE, 'DDMMYYYYHHMM')
                transactionId=safe_string(row[3]),           # atx.REFERENCE_NUMBER
                transactionType=safe_string(row[4]),         # CASE WHEN ... (Cash Withdrawal, etc.)
                currency=safe_string(row[5]),                # 'TZS'
                orgTransactionAmount=safe_float(row[6]),     # atx.TRANSACTION_AMOUNT
                tzsTransactionAmount=safe_float(row[7]),     # atx.TRANSACTION_AMOUNT (same as org)
                atmChannel=safe_string(row[8]),              # 'Card and Mobile Based'
                valueAddedTaxAmount=safe_float(row[9]),      # DECIMAL(atx.TRANSACTION_AMOUNT * 0.18, 15, 2)
                exciseDutyAmount=safe_float(row[10]),        # 0
                electronicLevyAmount=safe_float(row[11])     # 0
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing ATM transaction record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate ATM transaction record"""
        try:
            # Basic validation
            if not record.transactionId:
                self.logger.warning("Missing transaction ID")
                return False
            
            if not record.atmCode:
                self.logger.warning("Missing ATM code")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating ATM transaction record: {e}")
            return False
    
    def producer_thread(self, incremental=True):
        """Producer thread - executes query ONCE and streams results via fetchmany()"""
        try:
            self.logger.info("Producer thread started")
            
            last_timestamp = None
            if incremental:
                last_timestamp = self.get_last_successful_run()
                if last_timestamp:
                    self.logger.info(f"Incremental mode: fetching records with TMSTAMP > {last_timestamp}")
                else:
                    last_timestamp = 'epoch'
                    self.logger.info("Incremental mode: first run, fetching all records (epoch)")
            else:
                self.logger.info("Full load mode: fetching all records")
            
            # Get total count first
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total ATM transaction records available: {self.total_available:,}")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Execute the query ONCE and stream results
            query = self.get_atm_transactions_query(last_timestamp)
            self.logger.info("Executing ATM transactions query (single execution, streaming results)...")
            
            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                db2_cursor = db2_conn.cursor()
                db2_cursor.execute(query)
                self.logger.info("Query executed successfully, streaming results via fetchmany()...")
                
                batch_number = 1
                last_progress_report = time.time()
                
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
                                        routing_key='atm_transactions_queue',
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
                                        except Exception:
                                            pass
                                        connection, channel = self.setup_rabbitmq_connection()
                                        time.sleep(self.retry_delay)
                                    else:
                                        self.logger.error(f"Failed to publish message after {self.max_retries} attempts")
                            
                            if published:
                                batch_published += 1
                                self.total_produced += 1
                    
                    batch_time = time.time() - batch_start_time
                    progress_percent = self.total_produced / self.total_available * 100 if self.total_available > 0 else 0
                    
                    self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} rows fetched, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                    
                    # Progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        rate = self.total_produced / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - self.total_produced
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"PROGRESS REPORT: {self.total_produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                    batch_number += 1
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced:,} records published out of {self.total_available:,} available")
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
            insert_buffer = []
            pending_tags = []
            last_flush_time = time.time()
            flush_interval = 5  # seconds
            consumer_batch_size = 100
            last_progress_report = time.time()
            
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
                nonlocal insert_buffer, pending_tags, last_progress_report, last_flush_time
                try:
                    record_data = json.loads(body)
                    record = AtmTransactionRecord(**record_data)
                    
                    insert_buffer.append(record)
                    pending_tags.append(method.delivery_tag)
                    
                    # Flush if buffer is full or time interval exceeded
                    if len(insert_buffer) >= consumer_batch_size or \
                       time.time() - last_flush_time >= flush_interval:
                        flush_buffer(ch)
                    
                    # Progress monitoring
                    if self.total_consumed > 0 and self.total_consumed % self.batch_size == 0:
                        elapsed_time = time.time() - self.start_time
                        rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                        progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                        
                        self.logger.info(f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}% of total) - Rate: {rate:.1f} rec/sec")
                    
                    # Detailed progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - self.total_consumed if self.total_available > 0 else 0
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"CONSUMER PROGRESS: {self.total_consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS to match consumer batch size for efficient batching
            channel.basic_qos(prefetch_count=consumer_batch_size)
            channel.basic_consume(queue='atm_transactions_queue', on_message_callback=process_message)
            
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
                        
                        # Producer is done, check if queue is empty
                        queue_state = channel.queue_declare(queue='atm_transactions_queue', durable=True, passive=True)
                        if queue_state.method.message_count == 0:
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
                    channel.basic_qos(prefetch_count=consumer_batch_size)
                    channel.basic_consume(queue='atm_transactions_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
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
    
    def insert_batch_to_postgres(self, records, pg_conn):
        """Batch insert ATM transaction records to PostgreSQL with duplicate prevention"""
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "atmTransactions" (
                "reportingDate", "atmCode", "transactionDate", "transactionId", "transactionType",
                "currency", "orgTransactionAmount", "tzsTransactionAmount", "atmChannel",
                "valueAddedTaxAmount", "exciseDutyAmount", "electronicLevyAmount"
            ) VALUES %s
            ON CONFLICT ("transactionId") DO NOTHING
            """
            
            values = [
                (
                    r.reportingDate, r.atmCode, r.transactionDate, r.transactionId, r.transactionType,
                    r.currency, r.orgTransactionAmount, r.tzsTransactionAmount, r.atmChannel,
                    r.valueAddedTaxAmount, r.exciseDutyAmount, r.electronicLevyAmount
                )
                for r in records
            ]
            
            import psycopg2.extras
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} ATM transaction records: {e}")
            raise
    
    def ensure_unique_index(self):
        """Ensure unique index on transactionId exists for ON CONFLICT duplicate prevention"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_atm_transactions_id_unique
                    ON "atmTransactions" ("transactionId")
                """)
                conn.commit()
                self.logger.info("Unique index on transactionId verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on transactionId: {e}")
            raise
    
    def insert_to_postgres(self, record, cursor):
        """Insert ATM transaction record to PostgreSQL"""
        try:
            insert_query = """
            INSERT INTO "atmTransactions" (
                "reportingDate", "atmCode", "transactionDate", "transactionId", "transactionType",
                "currency", "orgTransactionAmount", "tzsTransactionAmount", "atmChannel",
                "valueAddedTaxAmount", "exciseDutyAmount", "electronicLevyAmount"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate,
                record.atmCode,
                record.transactionDate,
                record.transactionId,
                record.transactionType,
                record.currency,
                record.orgTransactionAmount,
                record.tzsTransactionAmount,
                record.atmChannel,
                record.valueAddedTaxAmount,
                record.exciseDutyAmount,
                record.electronicLevyAmount
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting ATM transaction record to PostgreSQL: {e}")
            raise
    
    def run_streaming_pipeline(self, incremental=True):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info(f"Starting ATM Transactions STREAMING pipeline... (incremental={incremental})")
        
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
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer", args=(incremental,))
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
            ATM Transactions Pipeline Summary:
            ==========================================
            Mode: {'Incremental' if incremental else 'Full Load'}
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/3600:.2f} hours
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
                'atm_transactions',
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
    
    parser = argparse.ArgumentParser(description='ATM Transactions Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = AtmTransactionsStreamingPipeline(batch_size=args.batch_size)
    
    try:
        if args.mode == 'producer':
            pipeline.producer_thread(incremental=not args.full_load)
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:  # streaming
            pipeline.run_streaming_pipeline(incremental=not args.full_load)
            
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()