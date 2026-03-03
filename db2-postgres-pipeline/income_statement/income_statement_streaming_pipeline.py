#!/usr/bin/env python3
"""
Income Statement Streaming Pipeline - Producer and Consumer run simultaneously
Uses income-statement.sql query for comprehensive income statement data extraction
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
class IncomeStatementRecord:
    """Data class for income statement records based on income-statement.sql"""
    reportingDate: str
    interestIncome: str  # JSON string format: [{"1": 5000000.00}, {"2": 200000.00}]
    interestIncomeValue: float
    interestExpenses: str  # JSON string format: [{"1": 3000000.00}, {"5": 100000.00}]
    interestExpensesValue: float
    badDebtsWrittenOffNotProvided: float
    provisionBadDoubtfulDebts: float
    impairmentsInvestments: float
    incomeTaxProvision: float
    extraordinaryCreditsCharge: float
    nonCoreCreditsCharges: str  # JSON string format: [{"1": 50000.00}, {"3": 120000.00}]
    nonCoreCreditsChargesValue: float
    nonInterestIncome: str  # JSON string format: [{"1": 200000.00}, {"4": 500000.00}]
    nonInterestIncomeValue: float
    nonInterestExpenses: str  # JSON string format: [{"1": 800000.00}, {"2": 2000000.00}]
    nonInterestExpensesValue: float


class IncomeStatementStreamingPipeline:
    def __init__(self, batch_size=1):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size  # Income statement typically returns 1 record
        
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
        
        self.logger.info("Income Statement STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_income_statement_query(self):
        """Get the income statement query using income-statement.sql"""
        
        # Read the income-statement.sql file
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'sqls', 'income-statement.sql')
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            query = f.read()
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available income statement records (always 1)"""
        return "VALUES (1)"
    
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
        """Setup RabbitMQ queue for income statement"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare queue with durability
            channel.queue_declare(queue='income_statement_queue', durable=True)
            
            connection.close()
            self.logger.info("RabbitMQ queue 'income_statement_queue' setup complete")
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single income statement record from DB2"""
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
                    return 0.0
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return 0.0
            
            # Map the fields from the SQL query to the dataclass
            record = IncomeStatementRecord(
                reportingDate=safe_string(row[0]),
                interestIncome=safe_string(row[1]),
                interestIncomeValue=safe_float(row[2]),
                interestExpenses=safe_string(row[3]),
                interestExpensesValue=safe_float(row[4]),
                badDebtsWrittenOffNotProvided=safe_float(row[5]),
                provisionBadDoubtfulDebts=safe_float(row[6]),
                impairmentsInvestments=safe_float(row[7]),
                incomeTaxProvision=safe_float(row[8]),
                extraordinaryCreditsCharge=safe_float(row[9]),
                nonCoreCreditsCharges=safe_string(row[10]),
                nonCoreCreditsChargesValue=safe_float(row[11]),
                nonInterestIncome=safe_string(row[12]),
                nonInterestIncomeValue=safe_float(row[13]),
                nonInterestExpenses=safe_string(row[14]),
                nonInterestExpensesValue=safe_float(row[15])
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing income statement record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate income statement record"""
        try:
            # Basic validation
            if not record.reportingDate:
                self.logger.warning("Missing reporting date")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating income statement record: {e}")
            return False
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total income statement records available: {self.total_available:,}")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Execute income statement query
            batch_start_time = time.time()
            
            # Fetch data with retry logic
            rows = None
            for attempt in range(self.max_retries):
                try:
                    with self.db2_conn.get_connection(log_connection=False) as conn:
                        cursor = conn.cursor()
                        query = self.get_income_statement_query()
                        cursor.execute(query)
                        rows = cursor.fetchall()
                    break
                except Exception as e:
                    self.logger.warning(f"DB2 query attempt {attempt + 1} failed: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    else:
                        raise
            
            if not rows:
                self.logger.info("No income statement records to process")
                connection.close()
                self.producer_finished.set()
                return
            
            # Process and publish records
            batch_published = 0
            for row in rows:
                # Process the row
                record = self.process_record(row)
                
                if self.validate_record(record):
                    message = json.dumps(asdict(record), default=str)
                    
                    # Publish with retry
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange='',
                                routing_key='income_statement_queue',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                            published = True
                            break
                        except Exception as e:
                            self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                # Reconnect and retry
                                try:
                                    connection.close()
                                except:
                                    pass
                                connection, channel = self.setup_rabbitmq_connection()
                                time.sleep(self.retry_delay)
                            else:
                                self.logger.error(f"Failed to publish message after {self.max_retries} attempts")
                    
                    if published:
                        batch_published += 1
                        self.total_produced += 1
            
            batch_time = time.time() - batch_start_time
            
            self.logger.info(f"Producer: {len(rows)} records, {batch_published} published (100% complete, {batch_time:.1f}s)")
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced:,} records published out of {self.total_available:,} available")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue with retry logic"""
        try:
            self.logger.info("Consumer thread started")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Initialize progress tracking variable
            last_progress_report = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal last_progress_report  # Allow modification of the outer variable
                try:
                    record_data = json.loads(body)
                    record = IncomeStatementRecord(**record_data)
                    
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
                            self.logger.warning(f"PostgreSQL insert attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                            else:
                                self.logger.error(f"Failed to insert record after {self.max_retries} attempts")
                    
                    if inserted:
                        self.total_consumed += 1
                        
                        # Progress monitoring
                        elapsed_time = time.time() - self.start_time
                        rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                        
                        self.logger.info(f"Consumer: Processed {self.total_consumed:,} records - Rate: {rate:.1f} rec/sec")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='income_statement_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='income_statement_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect
                    try:
                        connection.close()
                    except:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=1)
                    channel.basic_consume(queue='income_statement_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def insert_to_postgres(self, record, cursor):
        """Insert income statement record to PostgreSQL"""
        try:
            # Handle JSON conversion for JSON fields
            def safe_json(value):
                if not value:
                    return None
                try:
                    # If it's already a valid JSON string, parse and re-serialize to ensure validity
                    import json
                    if isinstance(value, str):
                        # Try to parse as JSON to validate
                        parsed = json.loads(value)
                        return json.dumps(parsed)
                    else:
                        return json.dumps(value)
                except (json.JSONDecodeError, TypeError):
                    # If not valid JSON, store as simple string in JSON format
                    return json.dumps({"raw_value": str(value)})
            
            insert_query = """
            INSERT INTO income_statement (
                "reportingDate", "interestIncome", "interestIncomeValue", "interestExpenses", "interestExpensesValue",
                "badDebtsWrittenOffNotProvided", "provisionBadDoubtfulDebts", "impairmentsInvestments",
                "incomeTaxProvision", "extraordinaryCreditsCharge", "nonCoreCreditsCharges", "nonCoreCreditsChargesValue",
                "nonInterestIncome", "nonInterestIncomeValue", "nonInterestExpenses", "nonInterestExpensesValue"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate,
                safe_json(record.interestIncome),
                record.interestIncomeValue,
                safe_json(record.interestExpenses),
                record.interestExpensesValue,
                record.badDebtsWrittenOffNotProvided,
                record.provisionBadDoubtfulDebts,
                record.impairmentsInvestments,
                record.incomeTaxProvision,
                record.extraordinaryCreditsCharge,
                safe_json(record.nonCoreCreditsCharges),
                record.nonCoreCreditsChargesValue,
                safe_json(record.nonInterestIncome),
                record.nonInterestIncomeValue,
                safe_json(record.nonInterestExpenses),
                record.nonInterestExpensesValue
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting income statement record to PostgreSQL: {e}")
            raise
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Income Statement STREAMING pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
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
            Income Statement Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/60:.2f} minutes
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Income Statement Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = IncomeStatementStreamingPipeline(batch_size=args.batch_size)
    
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
    main()