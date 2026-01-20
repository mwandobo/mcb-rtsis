#!/usr/bin/env python3
"""
Process the stuck message in RabbitMQ to see what error occurs
"""

import pika
import psycopg2
import json
import logging
from config import Config
from processors.agent_transaction_processor import AgentTransactionProcessor, AgentTransactionRecord

def process_stuck_message():
    """Process the stuck message to identify the error"""
    
    config = Config()
    processor = AgentTransactionProcessor()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    print("🔧 PROCESSING STUCK MESSAGE")
    print("=" * 50)
    
    try:
        # Connect to RabbitMQ
        credentials = pika.PlainCredentials(
            config.message_queue.rabbitmq_user,
            config.message_queue.rabbitmq_password
        )
        parameters = pika.ConnectionParameters(
            host=config.message_queue.rabbitmq_host,
            port=config.message_queue.rabbitmq_port,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        processed_count = 0
        
        def process_message(ch, method, properties, body):
            nonlocal processed_count
            try:
                print(f"\n📨 Processing message {processed_count + 1}:")
                
                # Parse the message
                record_data = json.loads(body)
                print(f"   Transaction ID: {record_data.get('transactionId', 'N/A')}")
                print(f"   Agent ID: {record_data.get('agentId', 'N/A')}")
                print(f"   Amount: {record_data.get('tzsAmount', 'N/A')}")
                
                # Create record
                record = AgentTransactionRecord(**record_data)
                
                # Try to insert to PostgreSQL
                cursor = pg_conn.cursor()
                
                print(f"   🔄 Attempting PostgreSQL insert...")
                processor.insert_to_postgres(record, cursor)
                pg_conn.commit()
                
                print(f"   ✅ Successfully inserted!")
                processed_count += 1
                
                # Acknowledge message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except psycopg2.IntegrityError as e:
                print(f"   ❌ PostgreSQL Integrity Error: {e}")
                print(f"   💡 This is likely a PRIMARY KEY conflict")
                pg_conn.rollback()
                ch.basic_ack(delivery_tag=method.delivery_tag)  # Remove the problematic message
                
            except Exception as e:
                print(f"   ❌ Processing Error: {e}")
                import traceback
                traceback.print_exc()
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        # Set QoS and consume
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='agent_transactions_queue', on_message_callback=process_message)
        
        # Check if there are messages to process
        method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        print(f"📊 Messages in queue: {message_count}")
        
        if message_count > 0:
            print(f"🔄 Processing {message_count} stuck messages...")
            
            # Process messages
            while message_count > 0:
                connection.process_data_events(time_limit=1)
                method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
                new_count = method.method.message_count
                if new_count == message_count:
                    break  # No progress, exit
                message_count = new_count
        
        connection.close()
        pg_conn.close()
        
        print(f"\n📊 RESULTS:")
        print(f"   Messages processed: {processed_count}")
        
        # Check final count in PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        cursor = pg_conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM "agentTransactions"')
        final_count = cursor.fetchone()[0]
        pg_conn.close()
        
        print(f"   Final PostgreSQL count: {final_count}")
        
    except Exception as e:
        print(f"❌ Failed to process stuck message: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    process_stuck_message()