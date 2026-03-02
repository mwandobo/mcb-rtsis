#!/usr/bin/env python3
"""
Balances BOT Streaming Pipeline - Producer and Consumer run simultaneously
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
class BalancesBotRecord:
    reportingDate: str
    accountNumber: str
    accountName: str
    accountType: str
    subAccountType: Optional[str]
    currency: str
    orgAmount: Optional[Decimal]
    usdAmount: Optional[Decimal]
    tzsAmount: Optional[Decimal]
    transactionDate: str
    maturityDate: str
    allowanceProbableLoss: int
    botProvision: int


class BalancesBotStreamingPipeline:
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
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Balances BOT STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_query(self, last_account_number=None):
        """Get the balances-bot query with cursor-based pagination"""
        
        where_clause = "WHERE gl.EXTERNAL_GLACCOUNT = '100028000'"
        
        if last_account_number:
            where_clause += f" AND gte.FK_GLG_ACCOUNTACCO > '{last_account_number}'"
        
        query = f"""
        select VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as reportingDate,
               gte.FK_GLG_ACCOUNTACCO                            as accountNumber,
               'BANK OF TANZANIA'                                as accountName,
               'TIPS'                                            as accountType,
               null                                              as subAccountType,
               gte.CURRENCY_SHORT_DES                            as currency,
               gte.DC_AMOUNT                                     AS orgAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN gte.DC_AMOUNT
                   ELSE NULL
                   END                                           AS usdAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN gte.DC_AMOUNT * 2500
                   ELSE
                       gte.DC_AMOUNT
                   END                                           AS tzsAmount,
               VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM')      as transactionDate,
               VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') as maturityDate,
               0                                                 as allowanceProbableLoss,
               0                                                 as botProvision,
               gte.FK_GLG_ACCOUNTACCO                            as cursor_account_number
        FROM GLI_TRX_EXTRACT AS gte
                 JOIN
             GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                 JOIN CUSTOMER c
                      ON c.CUST_ID = gte.CUST_ID
                 LEFT JOIN CURRENCY cu
                           ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))
        {where_clause}
        ORDER BY gte.FK_GLG_ACCOUNTACCO ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query

    def get_total_count_query(self):
        """Get total count of available records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT AS gte
                 JOIN
             GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                 JOIN CUSTOMER c
                      ON c.CUST_ID = gte.CUST_ID
                 LEFT JOIN CURRENCY cu
                           ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))
        WHERE gl.EXTERNAL_GLACCOUNT = '100028000'
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
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                return connection, channel
                
            except Exception as e:
                self.logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue='balances_bot_queue', durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'balances_bot_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record"""
        # Remove cursor field (last one)
        row_data = row[:-1]
        
        return BalancesBotRecord(
            reportingDate=str(row_data[0]) if row_data[0] else None,
            accountNumber=str(row_data[1]).strip() if row_data[1] else None,
            accountName=str(row_data[2]).strip() if row_data[2] else None,
            accountType=str(row_data[3]).strip() if row_data[3] else None,
            subAccountType=str(row_data[4]).strip() if row_data[4] else None,
            currency=str(row_data[5]).strip() if row_data[5] else None,
            orgAmount=Decimal(str(row_data[6])) if row_data[6] is not None else None,
            usdAmount=Decimal(str(row_data[7])) if row_data[7] is not None else None,
            tzsAmount=Decimal(str(row_data[8])) if row_data[8] is not None else None,
            transactionDate=str(row_data[9]) if row_data[9] else None,
            maturityDate=str(row_data[10]) if row_data[10] else None,
            allowanceProbableLoss=int(row_data[11]) if row_data[11] is not None else 0,
            botProvision=int(row_data[12]) if row_data[12] is not None else 0
        )
    
    def insert_to_postgres(self, record: BalancesBotRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "balancesBot" (
            "reportingDate", "accountNumber", "accountName", "accountType",
            "subAccountType", currency, "orgAmount", "usdAmount", "tzsAmount",
            "transactionDate", "maturityDate", "allowanceProbableLoss", "botProvision"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_sql, (
            record.reportingDate,
            record.accountNumber,
            record.accountName,
            record.accountType,
            record.subAccountType,
            record.currency,
            record.orgAmount,
            record.usdAmount,
            record.tzsAmount,
            record.transactionDate,
            record.maturityDate,
            record.allowanceProbableLoss,
            record.botProvision
        ))

    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total balances BOT records available: {self.total_available:,}")
            
            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches
            batch_number = 1
            last_account_number = None
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_query(last_account_number)
                            cursor.execute(batch_query)
                            rows = cursor.fetchall()
                        break
                    except Exception as e:
                        self.logger.warning(f"DB2 query attempt {attempt + 1} failed: {e}")
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
                    last_account_number = row[-1]  # cursor_account_number
                    
                    record = self.process_record(row)
                    message = json.dumps(asdict(record), default=str)
                    
                    # Publish
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange='',
                                routing_key='balances_bot_queue',
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
                                except:
                                    pass
                                connection, channel = self.setup_rabbitmq_connection()
                                time.sleep(self.retry_delay)
                    
                    if published:
                        batch_published += 1
                        self.total_produced += 1
                
                batch_time = time.time() - batch_start_time
                progress_percent = self.total_produced / self.total_available * 100 if self.total_available > 0 else 0
                
                self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                
                batch_number += 1
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced:,} records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()

    def consumer_thread(self):
        """Consumer thread - processes messages from queue"""
        try:
            self.logger.info("Consumer thread started - waiting for messages...")
            
            connection, channel = self.setup_rabbitmq_connection()
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = BalancesBotRecord(**record_data)
                    
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
                            self.logger.warning(f"PostgreSQL insert attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                    
                    if inserted:
                        self.total_consumed += 1
                        
                        if self.total_consumed % 10 == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                            self.logger.info(f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}%) - Rate: {rate:.1f} rec/sec")
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue='balances_bot_queue', on_message_callback=process_message)
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    if self.producer_finished.is_set():
                        method = channel.queue_declare(queue='balances_bot_queue', durable=True, passive=True)
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
                    channel.basic_consume(queue='balances_bot_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()

    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Balances BOT STREAMING pipeline...")
        
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
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Balances BOT Pipeline Summary:
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
    
    parser = argparse.ArgumentParser(description='Balances BOT Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=500, help='Batch size for processing')
    
    args = parser.parse_args()
    
    pipeline = BalancesBotStreamingPipeline(batch_size=args.batch_size)
    
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
