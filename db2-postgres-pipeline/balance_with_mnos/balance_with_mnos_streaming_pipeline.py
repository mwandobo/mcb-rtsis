#!/usr/bin/env python3
"""
Balance with MNOs Streaming Pipeline - Producer and Consumer run simultaneously
Based on balances-with-mnos.sql query
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
from decimal import Decimal
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class BalanceWithMnoRecord:
    """Data class for balance with MNO records based on balances-with-mnos.sql"""
    reportingDate: str
    floatBalanceDate: str
    mnoCode: Optional[str]
    tillNumber: Optional[str]
    currency: str
    allowanceProbableLoss: int
    botProvision: int
    orgFloatAmount: Optional[Decimal]
    usdFloatAmount: Optional[Decimal]
    tzsFloatAmount: Optional[Decimal]



class BalanceWithMnosStreamingPipeline:
    def __init__(self, batch_size=1000, consumer_batch_size=100):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = consumer_batch_size
        
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        self._stats_lock = threading.Lock()
        self.total_produced = 0
        self.total_consumed = 0
        self.total_available = 0
        self.start_time = time.time()
        
        self.max_retries = 3
        self.retry_delay = 5
        
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Balance with MNOs STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_balance_with_mnos_query(self, last_timestamp=None):
        """Get the balance with MNOs query from balances-with-mnos.sql"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'balances-with-mnos.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        if last_timestamp:
            if last_timestamp == 'epoch':
                timestamp_value = '1900-01-01-00.00.00.000000'
            else:
                timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]
            sql = sql.replace(':last_timestamp', f"'{timestamp_value}'")
        else:
            sql = sql.replace('AND gte.TMSTAMP > :last_timestamp', '')
        
        return sql
    
    def get_last_successful_run(self):
        """Get last successful run timestamp from state manager"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            return state_manager.get_last_successful_run('balance_with_mnos')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None
    
    def get_total_count(self, last_timestamp=None):
        """Get approximate total count of balance with MNO records from DB2"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                
                base_where = "WHERE gte.FK_GLG_ACCOUNTACCO IN ('1.4.4.00.0058', '1.4.4.00.0062')"
                count_query = f"""
                    SELECT COUNT(*) 
                    FROM GLI_TRX_EXTRACT gte
                    LEFT JOIN CURRENCY curr ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES
                    LEFT JOIN (
                        SELECT fr.fk_currencyid_curr, fr.rate
                        FROM fixing_rate fr
                        WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                              (SELECT fk_currencyid_curr, activation_date, MAX(activation_time)
                               FROM fixing_rate
                               WHERE activation_date = (SELECT MAX(b.activation_date)
                                                        FROM fixing_rate b
                                                        WHERE b.activation_date <= CURRENT_DATE)
                               GROUP BY fk_currencyid_curr, activation_date)
                    ) fx ON fx.fk_currencyid_curr = curr.ID_CURRENCY
                    LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
                    {base_where}
                """
                
                if last_timestamp and last_timestamp != 'epoch':
                    if last_timestamp == 'epoch':
                        timestamp_value = '1900-01-01-00.00.00.000000'
                    else:
                        timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]
                    count_query += f" AND gte.TMSTAMP > '{timestamp_value}'"
                
                cursor.execute(count_query)
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Record count: {count:,}")
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
                    heartbeat=600,
                    blocked_connection_timeout=300,
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
        """Setup RabbitMQ queue for balance with MNOs with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            channel.exchange_declare(exchange='balance_with_mnos_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='balance_with_mnos_dead_letter', durable=True)
            channel.queue_bind(
                queue='balance_with_mnos_dead_letter',
                exchange='balance_with_mnos_dlx',
                routing_key='balance_with_mnos_queue'
            )
            
            try:
                channel.queue_declare(
                    queue='balance_with_mnos_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'balance_with_mnos_dlx',
                        'x-dead-letter-routing-key': 'balance_with_mnos_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                self.logger.warning(
                    "Queue 'balance_with_mnos_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='balance_with_mnos_queue', durable=True)
                self.logger.info("RabbitMQ queue 'balance_with_mnos_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    
    def process_record(self, row):
        """Process a single balance with MNO record from DB2"""
        try:
            def safe_string(value):
                if value is None:
                    return None
                return str(value).strip()
            
            def safe_decimal(value):
                if value is None:
                    return None
                try:
                    return Decimal(str(value))
                except (ValueError, TypeError):
                    return None
            
            def safe_int(value):
                if value is None:
                    return 0
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return 0
            
            record = BalanceWithMnoRecord(
                reportingDate=safe_string(row[0]),
                floatBalanceDate=safe_string(row[1]),
                mnoCode=safe_string(row[2]) if row[2] else None,
                tillNumber=safe_string(row[3]) if row[3] else None,
                currency=safe_string(row[4]),
                allowanceProbableLoss=safe_int(row[5]),
                botProvision=safe_int(row[6]),
                orgFloatAmount=safe_decimal(row[7]),
                usdFloatAmount=safe_decimal(row[8]),
                tzsFloatAmount=safe_decimal(row[9])
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing balance with MNO record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate balance with MNO record"""
        try:
            if not record.currency:
                self.logger.warning("Missing currency")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating balance with MNO record: {e}")
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
            
            self.total_available = self.get_total_count(last_timestamp)
            
            self.logger.info(f"Total balance with MNO records available: {self.total_available:,}")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            rmq_connection, channel = self.setup_rabbitmq_connection()
            
            query = self.get_balance_with_mnos_query(last_timestamp)
            self.logger.info("Executing balance with MNOs query (single execution, streaming results)...")
            
            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                db2_cursor = db2_conn.cursor()
                db2_cursor.execute(query)
                self.logger.info("Query executed successfully, streaming results via fetchmany()...")
                
                batch_number = 1
                last_progress_report = time.time()
                
                while True:
                    batch_start_time = time.time()
                    
                    rows = db2_cursor.fetchmany(self.batch_size)
                    
                    if not rows:
                        self.logger.info("No more records to fetch")
                        break
                    
                    batch_published = 0
                    for row in rows:
                        record = self.process_record(row)
                        
                        if self.validate_record(record):
                            message = json.dumps(asdict(record), default=str)
                            
                            published = False
                            for attempt in range(self.max_retries):
                                try:
                                    channel.basic_publish(
                                        exchange='',
                                        routing_key='balance_with_mnos_queue',
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
                    
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        rate = produced / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - produced
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"PROGRESS REPORT: {produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
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
            
            pg_conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            self.logger.info("Consumer: Persistent PostgreSQL connection established")
            
            connection, channel = self.setup_rabbitmq_connection()
            
            insert_buffer: List[BalanceWithMnoRecord] = []
            pending_tags: List[int] = []
            last_flush_time = time.time()
            flush_interval = 5
            last_progress_report = time.time()
            
            def flush_buffer(ch):
                nonlocal insert_buffer, pending_tags, last_flush_time, pg_conn
                if not insert_buffer:
                    return
                
                batch_size = len(insert_buffer)
                try:
                    self.insert_batch_to_postgres(insert_buffer, pg_conn)
                    
                    if pending_tags:
                        ch.basic_ack(delivery_tag=pending_tags[-1], multiple=True)
                    
                    with self._stats_lock:
                        self.total_consumed += batch_size
                    
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
                    
                except psycopg2.OperationalError as e:
                    self.logger.error(f"PostgreSQL connection lost during batch insert: {e}")
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
                    record = BalanceWithMnoRecord(**record_data)
                    
                    insert_buffer.append(record)
                    pending_tags.append(method.delivery_tag)
                    
                    if len(insert_buffer) >= self.consumer_batch_size or \
                       time.time() - last_flush_time >= flush_interval:
                        flush_buffer(ch)
                    
                    with self._stats_lock:
                        consumed = self.total_consumed
                    
                    if consumed > 0 and consumed % self.batch_size == 0:
                        elapsed_time = time.time() - self.start_time
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        progress_percent = (consumed / self.total_available * 100) if self.total_available > 0 else 0
                        
                        self.logger.info(f"Consumer: Processed {consumed:,} records ({progress_percent:.2f}% of total) - Rate: {rate:.1f} rec/sec")
                    
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        with self._stats_lock:
                            consumed = self.total_consumed
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - consumed if self.total_available > 0 else 0
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"CONSUMER PROGRESS: {consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_qos(prefetch_count=self.consumer_batch_size)
            channel.basic_consume(queue='balance_with_mnos_queue', on_message_callback=process_message)
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    if insert_buffer and time.time() - last_flush_time >= flush_interval:
                        flush_buffer(channel)
                    
                    if self.producer_finished.is_set():
                        flush_buffer(channel)
                        
                        queue_state = channel.queue_declare(queue='balance_with_mnos_queue', durable=True, passive=True)
                        if queue_state.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='balance_with_mnos_queue', on_message_callback=process_message)
            
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

    
    def insert_batch_to_postgres(self, records: List[BalanceWithMnoRecord], pg_conn):
        """Batch insert balance with MNO records to PostgreSQL"""
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "balanceWithMnos" (
                "reportingDate", "floatBalanceDate", "mnoCode", "tillNumber", currency,
                "allowanceProbableLoss", "botProvision", "orgFloatAmount",
                "usdFloatAmount", "tzsFloatAmount"
            ) VALUES %s
            """
            
            values = [
                (
                    r.reportingDate, r.floatBalanceDate, r.mnoCode, r.tillNumber, r.currency,
                    r.allowanceProbableLoss, r.botProvision, r.orgFloatAmount,
                    r.usdFloatAmount, r.tzsFloatAmount
                )
                for r in records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} balance with MNO records: {e}")
            raise
    
    def run_streaming_pipeline(self, incremental=True):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info(f"Starting Balance with MNOs STREAMING pipeline... (incremental={incremental})")
        
        # Update state to running
        self._update_state('running')
        
        try:
            self.setup_rabbitmq_queue()
            
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            
            time.sleep(1)
            
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer", args=(incremental,))
            producer_thread.start()
            
            producer_thread.join()
            self.logger.info("Producer thread completed")
            
            consumer_thread.join(timeout=60)
            
            if consumer_thread.is_alive():
                self.logger.info("Stopping consumer thread...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)
            
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Balance with MNOs Pipeline Summary:
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
                'balance_with_mnos',
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
    
    parser = argparse.ArgumentParser(description='Balance with MNOs Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for DB2 query pagination')
    parser.add_argument('--consumer-batch-size', type=int, default=100, help='Batch size for PostgreSQL inserts')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true',
                       help='Run full load instead of incremental (ignores last run timestamp)')
    
    args = parser.parse_args()
    
    pipeline = BalanceWithMnosStreamingPipeline(batch_size=args.batch_size, consumer_batch_size=args.consumer_batch_size)
    
    try:
        if args.mode == 'producer':
            pipeline.producer_thread(incremental=not args.full_load)
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:
            pipeline.run_streaming_pipeline(incremental=not args.full_load)
            
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
