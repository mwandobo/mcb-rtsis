#!/usr/bin/env python3
"""
Agent Transactions Batched Pipeline - Processes ALL data in batches of 10
"""

import pika
import psycopg2
import json
import logging
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from db2_connection import DB2Connection
from processors.agent_transaction_processor import AgentTransactionProcessor, AgentTransactionRecord

class AgentTransactionsBatchedPipeline:
    def __init__(self, manual_start_date="2024-01-01 00:00:00", batch_size=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.agent_transaction_processor = AgentTransactionProcessor()
        self.manual_start_date = manual_start_date
        self.batch_size = batch_size
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("🏪 Agent Transactions BATCHED Pipeline initialized")
        self.logger.info(f"📅 Manual start date: {manual_start_date}")
        self.logger.info(f"📊 Batch size: {batch_size} records per batch")
    
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
            
            # Declare agent transactions queue
            channel.queue_declare(queue='agent_transactions_queue', durable=True)
            
            connection.close()
            self.logger.info("✅ RabbitMQ agent transactions queue created")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def get_total_available_records(self):
        """Get total count of available records"""
        try:
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                
                count_query = self.get_total_count_query()
                cursor.execute(count_query)
                total_count = cursor.fetchone()[0]
                
                return total_count
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def fetch_and_publish_batch(self, batch_number, offset):
        """Fetch one batch of data and publish to queue"""
        try:
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                
                batch_query = self.get_agent_transactions_query(offset)
                self.logger.info(f"📊 Executing batch {batch_number} query (offset: {offset})...")
                
                cursor.execute(batch_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"🏪 Batch {batch_number}: Fetched {len(rows)} records")
                
                if not rows:
                    self.logger.info(f"ℹ️ Batch {batch_number}: No more records found")
                    return 0
                
                # Show sample data for first batch
                if batch_number == 1:
                    first_date = rows[0][3] if rows else None
                    last_date = rows[-1][3] if rows else None
                    self.logger.info(f"📅 Date range: {first_date} to {last_date}")
                    
                    self.logger.info("📋 Sample data from first batch:")
                    for i, row in enumerate(rows[:3], 1):
                        agent_id = row[1] if row[1] is not None else "N/A"
                        transaction_type = row[5] if row[5] is not None else "N/A"
                        amount = row[9] if row[9] is not None else 0
                        currency = row[8] if row[8] is not None else "N/A"
                        self.logger.info(f"  {i}. Date: {row[3]}, Agent: {agent_id.strip()}, Type: {transaction_type}")
                        self.logger.info(f"      Amount: {amount:,.2f} {currency.strip()}")
                
                # Process and publish (skip the row number column)
                records = []
                for row in rows:
                    # Remove the row number column (last column) before processing
                    row_without_rn = row[:-1]  # All columns except the last one (rn)
                    record = self.agent_transaction_processor.process_record(row_without_rn, 'agentTransactions')
                    if self.agent_transaction_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'agent_transactions_queue')
                    self.logger.info(f"✅ Batch {batch_number}: Published {len(records)} records to queue")
                
                return len(records)
                
        except Exception as e:
            self.logger.error(f"❌ Batch {batch_number} fetch error: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def publish_records(self, records, queue_name):
        """Publish records to RabbitMQ"""
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
            
            for record in records:
                message = json.dumps(asdict(record), default=str)
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Publish error to {queue_name}: {e}")
            raise
    
    def consume_and_insert_all(self):
        """Consume all messages and insert to PostgreSQL"""
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
            
            processed_count = 0
            
            def process_message(ch, method, properties, body):
                nonlocal processed_count
                try:
                    record_data = json.loads(body)
                    record = AgentTransactionRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.agent_transaction_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    processed_count += 1
                    
                    if processed_count % self.batch_size == 0:
                        self.logger.info(f"🏪 Processed {processed_count} records...")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS and consume
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='agent_transactions_queue', on_message_callback=process_message)
            
            self.logger.info("🔄 Starting to consume and insert all agent transaction records...")
            
            # Process all messages until queue is empty
            while True:
                method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
                message_count = method.method.message_count
                
                if message_count == 0:
                    break
                
                self.logger.info(f"📊 Processing {message_count} messages from queue...")
                
                # Process messages for a short time
                connection.process_data_events(time_limit=5)
            
            connection.close()
            self.logger.info(f"✅ Finished processing {processed_count} total records")
            return processed_count
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            raise
    
    def run_complete_batched_pipeline(self):
        """Run the complete batched pipeline: process ALL data in batches"""
        self.logger.info("🚀 Starting complete BATCHED agent transactions pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Get total available records
            total_available = self.get_total_available_records()
            self.logger.info(f"📊 Total available records: {total_available:,}")
            
            if total_available == 0:
                self.logger.info("ℹ️ No records available to process")
                return 0
            
            # Calculate expected batches
            expected_batches = (total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"📦 Expected batches: {expected_batches} (batch size: {self.batch_size})")
            
            # Process all batches
            total_published = 0
            batch_number = 1
            offset = 0
            
            while True:
                published_count = self.fetch_and_publish_batch(batch_number, offset)
                
                if published_count == 0:
                    self.logger.info(f"✅ Completed all batches - no more data")
                    break
                
                total_published += published_count
                self.logger.info(f"📦 Batch {batch_number} completed: {published_count} records")
                
                # Move to next batch
                batch_number += 1
                offset += self.batch_size
                
                # Safety check to prevent infinite loops
                if batch_number > expected_batches + 5:
                    self.logger.warning(f"⚠️ Safety break - processed {batch_number} batches")
                    break
            
            self.logger.info(f"📊 Total published across all batches: {total_published:,}")
            
            if total_published > 0:
                # Consume and insert all
                processed_count = self.consume_and_insert_all()
                
                self.logger.info(f"✅ BATCHED Pipeline completed successfully!")
                self.logger.info(f"📊 Total batches processed: {batch_number - 1}")
                self.logger.info(f"📊 Total published: {total_published:,}")
                self.logger.info(f"📊 Total processed: {processed_count:,}")
                return processed_count
            else:
                self.logger.info("ℹ️ No records to process")
                return 0
                
        except Exception as e:
            self.logger.error(f"❌ Batched pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("🏪 AGENT TRANSACTIONS BATCHED PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Processes ALL available data")
    print("  - Uses batches of 10 records each")
    print("  - camelCase table name: agentTransactions")
    print("  - camelCase field names")
    print("  - Continues until all data is processed")
    print("=" * 60)
    
    pipeline = AgentTransactionsBatchedPipeline("2024-01-01 00:00:00", 10)
    
    try:
        count = pipeline.run_complete_batched_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ BATCHED PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"📊 Total records processed: {count:,}")
        print("🔍 Key features:")
        print("  - Processed ALL available data in batches")
        print("  - Batch size: 10 records per batch")
        print("  - Table: agentTransactions (camelCase)")
        print("  - Fields: reportingDate, agentId, transactionDate, etc.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Batched pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()