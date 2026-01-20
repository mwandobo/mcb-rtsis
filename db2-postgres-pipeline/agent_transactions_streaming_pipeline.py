#!/usr/bin/env python3
"""
Agent Transactions Streaming Pipeline - Producer and Consumer run simultaneously
"""

import pika
import psycopg2
import json
import logging
import threading
import time
from dataclasses import asdict
from contextlib import contextmanager
from queue import Queue, Empty

from config import Config
from db2_connection import DB2Connection
from processors.agent_transaction_processor import AgentTransactionProcessor, AgentTransactionRecord

class AgentTransactionsStreamingPipeline:
    def __init__(self, manual_start_date="2024-01-01 00:00:00", batch_size=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.agent_transaction_processor = AgentTransactionProcessor()
        self.manual_start_date = manual_start_date
        self.batch_size = batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("🏪 Agent Transactions STREAMING Pipeline initialized")
        self.logger.info(f"📅 Manual start date: {manual_start_date}")
        self.logger.info(f"📊 Batch size: {batch_size} records per batch")
        self.logger.info("🔄 Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_agent_transactions_query(self, offset=0):
        """Get the agent transactions query with ROW_NUMBER() for DB2 batching"""
        
        query = f"""
        SELECT * FROM (
            SELECT
                CURRENT_TIMESTAMP AS reportingDate,
                al.AGENT_ID AS agentId,
                'active' AS agentStatus,
                gte.TRN_DATE AS transactionDate,

                -- Proper transactionId construction
                VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
                TRIM(gte.FK_USRCODE) || '-' ||
                VARCHAR(gte.LINE_NUM) || '-' ||
                VARCHAR(gte.TRN_DATE) || '-' ||
                VARCHAR(gte.TRN_SNUM) AS transactionId,

                CASE
                    WHEN gl.EXTERNAL_GLACCOUNT = '230000079' THEN 'Cash Deposit'
                    WHEN gl.EXTERNAL_GLACCOUNT = '144000054' THEN 'Cash Withdraw'
                END AS transactionType,

                'Point of Sale' AS serviceChannel,
                NULL AS tillNumber,
                gte.CURRENCY_SHORT_DES AS currency,
                gte.DC_AMOUNT AS tzsAmount,
                
                ROW_NUMBER() OVER (ORDER BY gte.TRN_DATE ASC, gte.TRN_SNUM ASC) AS rn

            FROM GLI_TRX_EXTRACT gte
            INNER JOIN (
                SELECT DISTINCT
                    CASE
                        WHEN LENGTH(TRIM(TERMINAL_ID)) >= 8
                        THEN SUBSTR(TRIM(TERMINAL_ID), LENGTH(TRIM(TERMINAL_ID)) - 7, 8)
                        ELSE TRIM(TERMINAL_ID)
                    END AS TERMINAL_ID_8,
                    AGENT_ID
                FROM AGENTS_LIST
            ) al
                ON al.TERMINAL_ID_8 = TRIM(gte.TRX_USR)
            LEFT JOIN GLG_ACCOUNT gl
                ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('230000079', '144000054')
            AND gte.TRN_DATE >= TIMESTAMP('{self.manual_start_date}')
        ) numbered_results
        WHERE rn > {offset} AND rn <= {offset + self.batch_size}
        ORDER BY rn
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available records"""
        
        query = f"""
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT gte
        INNER JOIN (
            SELECT DISTINCT
                CASE
                    WHEN LENGTH(TRIM(TERMINAL_ID)) >= 8
                    THEN SUBSTR(TRIM(TERMINAL_ID), LENGTH(TRIM(TERMINAL_ID)) - 7, 8)
                    ELSE TRIM(TERMINAL_ID)
                END AS TERMINAL_ID_8,
                AGENT_ID
            FROM AGENTS_LIST
        ) al
            ON al.TERMINAL_ID_8 = TRIM(gte.TRX_USR)
        LEFT JOIN GLG_ACCOUNT gl
            ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        WHERE gl.EXTERNAL_GLACCOUNT IN ('230000079', '144000054')
        AND gte.TRN_DATE >= TIMESTAMP('{self.manual_start_date}')
        """
        
        return query
    
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
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for agent transactions"""
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
            
            # Purge existing queue
            try:
                channel.queue_purge('agent_transactions_queue')
                self.logger.info("🧹 Purged existing queue")
            except:
                pass
            
            # Declare agent transactions queue
            channel.queue_declare(queue='agent_transactions_queue', durable=True)
            
            connection.close()
            self.logger.info("✅ RabbitMQ agent transactions queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - fetches data and publishes to queue"""
        try:
            self.logger.info("🏭 Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                count_query = self.get_total_count_query()
                cursor.execute(count_query)
                total_available = cursor.fetchone()[0]
                
            self.logger.info(f"📊 Total available records: {total_available:,}")
            
            if total_available == 0:
                self.logger.info("ℹ️ No records available")
                self.producer_finished.set()
                return
            
            # Setup RabbitMQ connection for producer
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
            
            # Process batches
            batch_number = 1
            offset = 0
            
            while offset < total_available:
                # Fetch batch
                with self.db2_conn.get_connection() as conn:
                    cursor = conn.cursor()
                    batch_query = self.get_agent_transactions_query(offset)
                    cursor.execute(batch_query)
                    rows = cursor.fetchall()
                
                if not rows:
                    break
                
                self.logger.info(f"🏭 Producer: Batch {batch_number} - {len(rows)} records (offset: {offset})")
                
                # Process and publish immediately
                for row in rows:
                    row_without_rn = row[:-1]  # Remove row number
                    record = self.agent_transaction_processor.process_record(row_without_rn, 'agentTransactions')
                    
                    if self.agent_transaction_processor.validate_record(record):
                        message = json.dumps(asdict(record), default=str)
                        channel.basic_publish(
                            exchange='',
                            routing_key='agent_transactions_queue',
                            body=message,
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        self.total_produced += 1
                
                self.logger.info(f"🏭 Producer: Published batch {batch_number} ({self.total_produced} total)")
                
                # Move to next batch
                batch_number += 1
                offset += self.batch_size
                
                # Small delay to prevent overwhelming
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"🏭 Producer finished: {self.total_produced} records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue"""
        try:
            self.logger.info("🏪 Consumer thread started")
            
            # Setup RabbitMQ connection for consumer
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
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = AgentTransactionRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.agent_transaction_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    self.total_consumed += 1
                    
                    if self.total_consumed % self.batch_size == 0:
                        self.logger.info(f"🏪 Consumer: Processed {self.total_consumed} records")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=5)  # Process 5 messages at a time
            channel.basic_consume(queue='agent_transactions_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("🏪 Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"❌ Consumer processing error: {e}")
                    break
            
            connection.close()
            self.logger.info(f"🏪 Consumer finished: {self.total_consumed} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("🚀 Starting STREAMING agent transactions pipeline...")
        
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
            self.logger.info("✅ Producer thread completed")
            
            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=30)  # 30 second timeout
            
            if consumer_thread.is_alive():
                self.logger.warning("⚠️ Consumer thread timeout, stopping...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=5)
            
            self.logger.info("✅ Consumer thread completed")
            
            self.logger.info(f"📊 STREAMING Pipeline Results:")
            self.logger.info(f"   Produced: {self.total_produced:,} records")
            self.logger.info(f"   Consumed: {self.total_consumed:,} records")
            
            return self.total_consumed
            
        except Exception as e:
            self.logger.error(f"❌ Streaming pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("🏪 AGENT TRANSACTIONS STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Minimal queue accumulation")
    print("  - Batch size: 10 records per batch")
    print("  - camelCase table and field names")
    print("=" * 60)
    
    pipeline = AgentTransactionsStreamingPipeline("2024-01-01 00:00:00", 10)
    
    try:
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ STREAMING PIPELINE COMPLETED!")
        print(f"📊 Total records processed: {count:,}")
        print("🔍 Key advantages:")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Memory efficient")
        print("  - Fast processing")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Streaming pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()