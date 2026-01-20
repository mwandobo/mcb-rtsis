#!/usr/bin/env python3
"""
Agent Transactions Pipeline with camelCase naming
Uses the query from sqls/agent-transactions.sql
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

class AgentTransactionsPipeline:
    def __init__(self, manual_start_date="2024-01-01 00:00:00", limit=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.agent_transaction_processor = AgentTransactionProcessor()
        self.manual_start_date = manual_start_date
        self.limit = limit
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("🏪 Agent Transactions Pipeline initialized")
        self.logger.info(f"📅 Manual start date: {manual_start_date}")
        self.logger.info(f"📊 Record limit: {limit}")
    
    def get_agent_transactions_query(self):
        """Get the agent transactions query with camelCase field mapping"""
        
        timestamp_filter = f"AND gte.TRN_DATE >= TIMESTAMP('{self.manual_start_date}')"
        
        query = f"""
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
            gte.DC_AMOUNT AS tzsAmount

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
        {timestamp_filter}
        ORDER BY gte.TRN_DATE ASC
        FETCH FIRST {self.limit} ROWS ONLY
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
    
    def fetch_and_publish_agent_transactions(self):
        """Fetch agent transaction data and publish to queue"""
        try:
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                
                agent_transactions_query = self.get_agent_transactions_query()
                self.logger.info("📊 Executing agent transactions query...")
                
                cursor.execute(agent_transactions_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"🏪 Fetched {len(rows)} agent transaction records")
                
                if not rows:
                    self.logger.info("ℹ️ No agent transaction records found")
                    return 0
                
                # Show date range of fetched data
                first_date = rows[0][3]  # transactionDate is at index 3
                last_date = rows[-1][3]
                self.logger.info(f"📅 Date range: {first_date} to {last_date}")
                
                # Show sample data
                self.logger.info("📋 Sample agent transaction data:")
                for i, row in enumerate(rows[:3], 1):
                    agent_id = row[1] if row[1] is not None else "N/A"
                    transaction_type = row[5] if row[5] is not None else "N/A"
                    amount = row[9] if row[9] is not None else 0
                    currency = row[8] if row[8] is not None else "N/A"
                    self.logger.info(f"  {i}. Date: {row[3]}, Agent: {agent_id}, Type: {transaction_type}")
                    self.logger.info(f"      Amount: {amount:,.2f} {currency}, Channel: {row[6]}")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.agent_transaction_processor.process_record(row, 'agentTransactions')
                    if self.agent_transaction_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'agent_transactions_queue')
                    self.logger.info(f"✅ Published {len(records)} agent transaction records to queue")
                
                return len(records)
                
        except Exception as e:
            self.logger.error(f"❌ Agent transactions fetch error: {e}")
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
    
    def consume_and_insert(self):
        """Consume messages and insert directly to PostgreSQL"""
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
                    
                    if processed_count % 100 == 0:
                        self.logger.info(f"🏪 Processed {processed_count} agent transaction records...")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS and consume
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='agent_transactions_queue', on_message_callback=process_message)
            
            self.logger.info("🔄 Starting to consume and insert agent transaction records...")
            
            # Check if there are messages to process
            method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
            message_count = method.method.message_count
            
            if message_count > 0:
                self.logger.info(f"📊 Processing {message_count} messages from queue...")
                
                # Process all messages
                while message_count > 0:
                    connection.process_data_events(time_limit=1)
                    method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
                    new_count = method.method.message_count
                    if new_count == message_count:
                        break  # No progress, exit
                    message_count = new_count
            
            connection.close()
            self.logger.info(f"✅ Finished processing {processed_count} agent transaction records")
            return processed_count
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            raise
    
    def run_complete_pipeline(self):
        """Run the complete pipeline: fetch, publish, consume, insert"""
        self.logger.info("🚀 Starting complete agent transactions pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Fetch and publish
            published_count = self.fetch_and_publish_agent_transactions()
            
            if published_count > 0:
                # Consume and insert
                processed_count = self.consume_and_insert()
                
                self.logger.info(f"✅ Pipeline completed successfully!")
                self.logger.info(f"📊 Published: {published_count}, Processed: {processed_count}")
                return processed_count
            else:
                self.logger.info("ℹ️ No records to process")
                return 0
                
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("🏪 AGENT TRANSACTIONS PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - camelCase table name: agentTransactions")
    print("  - camelCase field names")
    print("  - Uses sqls/agent-transactions.sql query")
    print("  - Processes Cash Deposit and Cash Withdraw transactions")
    print("  - Default batch size: 10 records")
    print("=" * 60)
    
    pipeline = AgentTransactionsPipeline("2024-01-01 00:00:00", 10)
    
    try:
        count = pipeline.run_complete_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"📊 Total records processed: {count:,}")
        print("🔍 Key features:")
        print("  - Table: agentTransactions (camelCase)")
        print("  - Fields: reportingDate, agentId, transactionDate, etc.")
        print("  - Transaction types: Cash Deposit, Cash Withdraw")
        print("  - Service channel: Point of Sale")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()