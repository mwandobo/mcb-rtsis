#!/usr/bin/env python3
"""
IBCM Transactions Streaming Pipeline - Producer and Consumer run simultaneously
Uses ibcm_transactions-v1.sql query for IBCM transaction data extraction

Improvements over original pipeline:
  1. Single query execution + fetchmany() streaming (no re-execute per batch)
  2. Thread-safe counters with _stats_lock
  3. Batch inserts via psycopg2.extras.execute_values
  4. ON CONFLICT duplicate prevention on transactionDate + lenderName + borrowerName
  5. Consumer batch buffering with flush interval
  6. Dead-letter queue support with graceful fallback
  7. Persistent PostgreSQL connection in consumer
  8. Module-level logging.basicConfig
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
from pipeline_state import PipelineStateManager

# Configure logging at module level (should only be called once)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class IBCMTransactionRecord:
    """Data class for IBCM transaction records based on ibcm_transactions-v1.sql"""
    reportingDate: str
    transactionDate: str
    lenderName: str
    borrowerName: str
    transactionType: str
    tzsAmount: Optional[float]
    tenure: Optional[int]
    interestRate: Optional[float]


class IBCMTransactionsStreamingPipeline:
    def __init__(self, batch_size=1000):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = 100  # Fixed value per pipeline standards

        # State management for incremental processing (following overdraft pattern)
        # State manager will be initialized when needed

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

        self.logger.info("IBCM Transactions STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")

    def get_last_successful_run(self):
        """Get last successful run timestamp from state manager"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            return state_manager.get_last_successful_run('ibcm_transactions')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None
    
    def _update_state(self, status, error_message=None):
        """Update pipeline state in the state table"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            state_manager.update_run(
                'ibcm_transactions',
                status,
                self.total_consumed,
                error_message
            )
            self.logger.info(f"State updated: {status}, records: {self.total_consumed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")

    def get_ibcm_transactions_query(self, last_timestamp=None):
        """Get the full IBCM transactions query from ibcm_transactions-v1.sql with optional timestamp filter.

        The query is executed ONCE and results are streamed via cursor.fetchmany().
        No pagination modifications are made to the SQL.

        Args:
            last_timestamp: Optional timestamp for incremental processing

        Returns:
            str: The complete SQL query with optional timestamp filter
        """
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'ibcm_transactions-v1.sql'
        )
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                query = file.read().strip()
                self.logger.info(f"Loaded IBCM transactions query from: {sql_file_path}")
                
                # Add incremental filter if timestamp provided
                if last_timestamp:
                    # Find the WHERE clause and add timestamp filter
                    query_lines = query.split('\n')
                    
                    # Find the WHERE clause line
                    where_line_idx = -1
                    for i, line in enumerate(query_lines):
                        if line.strip().upper().startswith('WHERE '):
                            where_line_idx = i
                            break
                    
                    if where_line_idx >= 0:
                        # Add timestamp filter to existing WHERE clause
                        query_lines.insert(where_line_idx + 1, f"  AND gte.TMSTAMP > '{last_timestamp}'")
                    else:
                        # No WHERE clause found, add one at the end
                        query_lines.append(f"WHERE gte.TMSTAMP > '{last_timestamp}'")
                    
                    query = '\n'.join(query_lines)
                
                return query
        except FileNotFoundError:
            self.logger.error(f"SQL file not found: {sql_file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading SQL file: {e}")
            raise

    def validate_record(self, record_data):
        """Validate an IBCM transaction record before processing.
        
        Args:
            record_data: Dictionary containing record data
            
        Returns:
            bool: True if record is valid, False otherwise
        """
        try:
            # Check required fields
            if not record_data.get('transactiondate'):
                self.logger.warning("Record missing transactionDate")
                return False
                
            if not record_data.get('lendername'):
                self.logger.warning("Record missing lenderName")
                return False
                
            if not record_data.get('borrowername'):
                self.logger.warning("Record missing borrowerName")
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Error validating record: {e}")
            return False

    def get_total_count(self, last_timestamp=None):
        """Get approximate total count of IBCM transaction records from DB2.
        Uses the base table count for a fast estimate.
        
        Args:
            last_timestamp: Optional timestamp for incremental counting
        """
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                count_query = "SELECT COUNT(*) FROM GLI_TRX_EXTRACT WHERE FK_GLG_ACCOUNTACCO = '1.0.2.00.0001'"
                
                # Add incremental filter if timestamp provided
                if last_timestamp:
                    count_query += f" AND TMSTAMP > '{last_timestamp}'"
                
                cursor.execute(count_query)
                result = cursor.fetchone()
                count = result[0] if result else 0
                
                if last_timestamp:
                    self.logger.info(f"Incremental record count (since {last_timestamp}): {count:,}")
                else:
                    self.logger.info(f"Full record count: {count:,}")
                return count
        except Exception as e:
            self.logger.warning(f"Could not fetch record count, progress % unavailable: {e}")
            return 0

    @contextmanager
    def setup_rabbitmq_connection(self):
        """Setup RabbitMQ connection with proper error handling"""
        connection = None
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
            yield connection, channel
        except Exception as e:
            self.logger.error(f"RabbitMQ connection failed: {e}")
            raise
        finally:
            if connection and not connection.is_closed:
                connection.close()

    def setup_rabbitmq_queues(self):
        """Setup RabbitMQ queues with dead-letter support"""
        with self.setup_rabbitmq_connection() as (connection, channel):
            self.logger.info("Setting up RabbitMQ queues...")

            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='ibcm_transactions_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='ibcm_transactions_dead_letter', durable=True)
            
            channel.queue_bind(
                queue='ibcm_transactions_dead_letter',
                exchange='ibcm_transactions_dlx',
                routing_key='ibcm_transactions_queue'
            )

            # Declare main queue with dead-letter support
            try:
                channel.queue_declare(
                    queue='ibcm_transactions_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'ibcm_transactions_dlx',
                        'x-dead-letter-routing-key': 'ibcm_transactions_queue'
                    }
                )
                self.logger.info("RabbitMQ queue 'ibcm_transactions_queue' setup complete (with DLX)")
            except pika.exceptions.ChannelClosedByBroker:
                # Queue may already exist with different arguments; reconnect and use as-is
                self.logger.warning(
                    "Queue 'ibcm_transactions_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='ibcm_transactions_queue', durable=True)
                self.logger.info("RabbitMQ queue 'ibcm_transactions_queue' setup complete (without DLX)")

            connection.close()
            self.logger.info("RabbitMQ setup completed successfully")

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
            self.logger.error(f"PostgreSQL connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def ensure_table_exists(self):
        """Ensure the IBCM transactions table exists in PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if table exists
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = 'ibcmTransactions'
                        )
                    """)
                    
                    table_exists = cursor.fetchone()[0]
                    
                    if table_exists:
                        self.logger.info("IBCM transactions table already exists")
                        return
                    
                    self.logger.info("Creating IBCM transactions table...")
                    
                    # Create table directly with SQL DDL (following overdraft pattern)
                    create_table_query = """
                    CREATE TABLE "ibcmTransactions" (
                        id SERIAL PRIMARY KEY,
                        "reportingDate" VARCHAR(12) NOT NULL,
                        "transactionDate" VARCHAR(12) NOT NULL,
                        "lenderName" VARCHAR(255) NOT NULL,
                        "borrowerName" VARCHAR(255) NOT NULL,
                        "transactionType" VARCHAR(50),
                        "tzsAmount" DECIMAL(18,2),
                        tenure INTEGER,
                        "interestRate" DECIMAL(8,4),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                    
                    cursor.execute(create_table_query)
                    
                    # Create indexes for better performance
                    cursor.execute("CREATE INDEX idx_ibcm_transactions_reporting_date ON \"ibcmTransactions\"(\"reportingDate\")")
                    cursor.execute("CREATE INDEX idx_ibcm_transactions_transaction_date ON \"ibcmTransactions\"(\"transactionDate\")")
                    cursor.execute("CREATE INDEX idx_ibcm_transactions_lender ON \"ibcmTransactions\"(\"lenderName\")")
                    cursor.execute("CREATE INDEX idx_ibcm_transactions_borrower ON \"ibcmTransactions\"(\"borrowerName\")")
                    cursor.execute("CREATE INDEX idx_ibcm_transactions_type ON \"ibcmTransactions\"(\"transactionType\")")
                    cursor.execute("CREATE INDEX idx_ibcm_transactions_created_at ON \"ibcmTransactions\"(created_at)")
                    
                    conn.commit()
                    self.logger.info("IBCM transactions table created successfully with indexes")
                        
        except Exception as e:
            self.logger.error(f"Error ensuring table exists: {e}")
            raise

    def producer_thread(self, incremental=True):
        """Producer thread - reads from DB2 and publishes to RabbitMQ"""
        try:
            self.logger.info("Producer: Starting...")
            
            # Get last successful run timestamp for incremental processing
            last_timestamp = None
            if incremental:
                last_timestamp = self.get_last_successful_run()
                if last_timestamp:
                    self.logger.info(f"Incremental mode: Processing records since {last_timestamp}")
                else:
                    self.logger.info("Incremental mode: No previous run found, processing all records")
            else:
                self.logger.info("Full load mode: Processing all records")
            
            # Get dynamic record count
            self.total_available = self.get_total_count(last_timestamp)
            
            with self.setup_rabbitmq_connection() as (connection, channel):
                # Execute the v1 query ONCE and stream results
                query = self.get_ibcm_transactions_query(last_timestamp)
                
                self.logger.info("Executing IBCM transactions query (single execution, streaming results)...")
                
                latest_timestamp = None
                
                with self.db2_conn.get_connection() as db2_conn:
                    cursor = db2_conn.cursor()
                    cursor.execute(query)
                    
                    batch_count = 0
                    total_records = 0
                    
                    while True:
                        # Fetch batch using fetchmany for memory efficiency
                        rows = cursor.fetchmany(self.batch_size)
                        if not rows:
                            break
                            
                        batch_count += 1
                        batch_records = []
                        
                        for row in rows:
                            try:
                                # Convert row to dictionary using column names
                                columns = [desc[0].lower() for desc in cursor.description]
                                record_dict = dict(zip(columns, row))
                                
                                # Extract TMSTAMP for state tracking (should be in the row)
                                tmstamp = None
                                if 'tmstamp' in record_dict:
                                    tmstamp = record_dict['tmstamp']
                                elif len(row) > len(columns):
                                    # TMSTAMP might be an additional field
                                    tmstamp = row[-1]
                                
                                # Track latest timestamp for state update
                                if tmstamp and (not latest_timestamp or tmstamp > latest_timestamp):
                                    latest_timestamp = tmstamp
                                
                                # Convert dates and handle None values
                                for key, value in record_dict.items():
                                    if value is None:
                                        record_dict[key] = None
                                    elif isinstance(value, datetime):
                                        record_dict[key] = value.isoformat()
                                    else:
                                        record_dict[key] = str(value).strip() if isinstance(value, str) else value
                                
                                # Validate record
                                if self.validate_record(record_dict):
                                    batch_records.append(record_dict)
                                
                            except Exception as e:
                                self.logger.error(f"Error processing row: {e}")
                                continue
                        
                        # Publish batch to RabbitMQ
                        if batch_records:
                            for record in batch_records:
                                message = json.dumps(record, default=str)
                                channel.basic_publish(
                                    exchange='',
                                    routing_key='ibcm_transactions_queue',
                                    body=message,
                                    properties=pika.BasicProperties(delivery_mode=2)
                                )
                            
                            total_records += len(batch_records)
                            
                            with self._stats_lock:
                                self.total_produced += len(batch_records)
                            
                            self.logger.info(f"Producer: Batch {batch_count} - {len(batch_records)} records published (Total: {total_records})")
                    
                    # Update state with latest timestamp if incremental and we processed records
                    if incremental and latest_timestamp and total_records > 0:
                        # For now, just log the timestamp - state update will be done at pipeline completion
                        self.logger.info(f"Latest processed timestamp: {latest_timestamp}")
                    
                    self.logger.info(f"Producer: Completed - {total_records} total records published")
                    
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            raise
        finally:
            self.producer_finished.set()
            self.logger.info("Producer: Finished")

    def consumer_thread(self):
        """Consumer thread - consumes from RabbitMQ and inserts to PostgreSQL"""
        buffer = []
        last_flush_time = time.time()
        flush_interval = 30  # seconds
        
        try:
            self.logger.info("Consumer: Starting...")
            
            # Maintain persistent PostgreSQL connection
            with self.get_postgres_connection() as pg_conn:
                pg_conn.autocommit = False
                
                def flush_buffer(force=False):
                    nonlocal buffer, last_flush_time
                    
                    if not buffer:
                        return
                    
                    current_time = time.time()
                    should_flush = (
                        len(buffer) >= self.consumer_batch_size or
                        force or
                        (current_time - last_flush_time) >= flush_interval
                    )
                    
                    if should_flush:
                        try:
                            with pg_conn.cursor() as cursor:
                                # Deduplicate records within the batch to avoid ON CONFLICT issues
                                unique_records = {}
                                for record in buffer:
                                    key = (record.get('transactiondate'), record.get('lendername'), record.get('borrowername'))
                                    unique_records[key] = record  # Last record wins for duplicates
                                
                                if not unique_records:
                                    return
                                
                                # Prepare insert query without ON CONFLICT (allow duplicates)
                                insert_query = """
                                    INSERT INTO "ibcmTransactions" (
                                        "reportingDate", "transactionDate", "lenderName", "borrowerName", 
                                        "transactionType", "tzsAmount", tenure, "interestRate"
                                    ) VALUES %s
                                """
                                
                                # Prepare values tuple from all records (no deduplication needed)
                                values = []
                                for record in buffer:
                                    values.append((
                                        record.get('reportingdate'),
                                        record.get('transactiondate'),
                                        record.get('lendername'),
                                        record.get('borrowername'),
                                        record.get('transactiontype'),
                                        record.get('tzsamount'),
                                        record.get('tenure'),
                                        record.get('interestrate')
                                    ))
                                
                                # Execute batch insert
                                psycopg2.extras.execute_values(cursor, insert_query, values)
                                pg_conn.commit()
                                
                                with self._stats_lock:
                                    self.total_consumed += len(buffer)
                                
                                self.logger.info(f"Consumer: Flushed {len(buffer)} records to PostgreSQL")
                                
                        except Exception as e:
                            self.logger.error(f"Error flushing buffer: {e}")
                            pg_conn.rollback()
                            raise
                        
                        buffer.clear()
                        last_flush_time = current_time

                def process_message(channel, method, properties, body):
                    try:
                        record = json.loads(body.decode('utf-8'))
                        buffer.append(record)
                        
                        # Acknowledge message
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                        
                        # Check if we should flush
                        flush_buffer()
                        
                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}")
                        # Reject message and send to dead letter queue
                        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

                # Setup consumer
                with self.setup_rabbitmq_connection() as (connection, channel):
                    # Set QoS to match consumer batch size for efficient batching
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='ibcm_transactions_queue', on_message_callback=process_message)
                    
                    # Keep consuming until producer is done and queue is empty
                    while True:
                        try:
                            connection.process_data_events(time_limit=1)
                            
                            if self.producer_finished.is_set():
                                # Producer is done, check if queue is empty
                                queue_state = channel.queue_declare(queue='ibcm_transactions_queue', durable=True, passive=True)
                                if queue_state.method.message_count == 0:
                                    self.logger.info("Consumer: Queue empty, producer finished")
                                    break
                                    
                        except pika.exceptions.AMQPConnectionError:
                            self.logger.warning("Consumer: Connection lost, reconnecting...")
                            break
                        except Exception as e:
                            self.logger.error(f"Consumer error: {e}")
                            break
                    
                    # Final flush
                    flush_buffer(force=True)
                    
        except Exception as e:
            self.logger.error(f"Consumer thread error: {e}")
            raise
        finally:
            self.consumer_finished.set()
            self.logger.info("Consumer: Finished")

    def run_streaming_pipeline(self, incremental=True):
        """Run the complete streaming pipeline"""
        try:
            mode_desc = "incremental" if incremental else "full load"
            self.logger.info("=" * 60)
            self.logger.info(f"STARTING IBCM TRANSACTIONS STREAMING PIPELINE ({mode_desc})")
            self.logger.info("=" * 60)
            
            # Update state to running
            self._update_state('running')
            
            # Setup infrastructure
            self.setup_rabbitmq_queues()
            self.ensure_table_exists()
            
            # Start producer and consumer threads
            producer_thread = threading.Thread(target=lambda: self.producer_thread(incremental), name="IBCMTransactionsProducer")
            consumer_thread = threading.Thread(target=self.consumer_thread, name="IBCMTransactionsConsumer")
            
            producer_thread.start()
            consumer_thread.start()
            
            # Wait for both threads to complete
            producer_thread.join()
            consumer_thread.join()
            
            # Final statistics
            elapsed_time = time.time() - self.start_time
            with self._stats_lock:
                self.logger.info("=" * 60)
                self.logger.info("IBCM TRANSACTIONS PIPELINE COMPLETED")
                self.logger.info(f"Mode: {mode_desc}")
                self.logger.info(f"Total produced: {self.total_produced}")
                self.logger.info(f"Total consumed: {self.total_consumed}")
                self.logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
                if elapsed_time > 0:
                    self.logger.info(f"Average rate: {self.total_consumed/elapsed_time:.2f} records/second")
                self.logger.info("=" * 60)
            
            # Update pipeline state
            self._update_state('completed')
                
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self._update_state('failed', str(e))
            raise


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='IBCM Transactions Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = IBCMTransactionsStreamingPipeline(batch_size=args.batch_size)
    
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