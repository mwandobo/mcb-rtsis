#!/usr/bin/env python3
"""
Test Agents Pipeline with RabbitMQ Integration
"""

import sys
import os
import logging
import time
import json
from datetime import datetime

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.agent_processor import AgentProcessor
import pika
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_agents_rabbitmq_pipeline():
    """Test the complete agents pipeline with RabbitMQ"""
    config = Config()
    
    logger.info("🚀 Starting Agents RabbitMQ Pipeline Test")
    
    # Step 1: Extract data from DB2 and publish to RabbitMQ
    logger.info("📤 Step 1: Extracting agents data from DB2 and publishing to RabbitMQ...")
    
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config.message_queue.rabbitmq_host,
                port=config.message_queue.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    config.message_queue.rabbitmq_user,
                    config.message_queue.rabbitmq_password
                )
            )
        )
        channel = connection.channel()
        
        # Declare the agents queue
        agents_config = config.tables['agents']
        queue_name = agents_config.queue_name
        
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"✅ Declared queue: {queue_name}")
        
        # Connect to DB2 and extract data
        db2_conn = DB2Connection()
        published_count = 0
        
        with db2_conn.get_connection() as db2_connection:
            cursor = db2_connection.cursor()
            
            logger.info("🔍 Executing agents query...")
            cursor.execute(agents_config.query)
            
            results = cursor.fetchall()
            logger.info(f"📊 Found {len(results)} agent records in DB2")
            
            # Publish each record to RabbitMQ
            for record in results:
                message = {
                    'table_name': 'agents',
                    'data': record,
                    'timestamp': datetime.now().isoformat()
                }
                
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=json.dumps(message, default=str),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Make message persistent
                    )
                )
                published_count += 1
            
            logger.info(f"📤 Published {published_count} agent records to RabbitMQ")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"❌ Error in Step 1: {e}")
        return False
    
    # Step 2: Consume from RabbitMQ and process to PostgreSQL
    logger.info("📥 Step 2: Consuming from RabbitMQ and processing to PostgreSQL...")
    
    try:
        # Connect to RabbitMQ for consuming
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config.message_queue.rabbitmq_host,
                port=config.message_queue.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    config.message_queue.rabbitmq_user,
                    config.message_queue.rabbitmq_password
                )
            )
        )
        channel = connection.channel()
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        
        # Initialize processor
        processor = AgentProcessor()
        processed_count = 0
        
        def process_message(ch, method, properties, body):
            nonlocal processed_count
            try:
                message = json.loads(body)
                raw_data = tuple(message['data'])
                
                # Process the record
                agent_record = processor.process_record(raw_data, 'agents')
                
                if processor.validate_record(agent_record):
                    # Insert to PostgreSQL
                    processor.insert_to_postgres(agent_record, pg_cursor)
                    pg_conn.commit()
                    
                    processed_count += 1
                    logger.info(f"✅ Processed agent: {agent_record.agent_name} (ID: {agent_record.agent_id})")
                    
                    # Acknowledge the message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logger.warning(f"⚠️ Invalid agent record: {raw_data[1]}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        # Set up consumer
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=process_message)
        
        logger.info("🔄 Starting to consume messages...")
        
        # Consume messages with timeout
        start_time = time.time()
        timeout = 30  # 30 seconds timeout
        
        while time.time() - start_time < timeout:
            connection.process_data_events(time_limit=1)
            
            # Check if queue is empty
            method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=False)
            if method_frame is None:
                logger.info("📭 Queue is empty, stopping consumer")
                break
            else:
                # Put the message back and process it
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                connection.process_data_events(time_limit=1)
        
        logger.info(f"📥 Processed {processed_count} agent records from RabbitMQ")
        
        # Verify data in PostgreSQL
        pg_cursor.execute('SELECT COUNT(*) FROM "agents"')
        total_count = pg_cursor.fetchone()[0]
        logger.info(f"📊 Total agents in PostgreSQL: {total_count}")
        
        # Show recent agents
        pg_cursor.execute('''
            SELECT "agentName", "agentId", "agentStatus", "agentType", "region", "district" 
            FROM "agents" 
            ORDER BY "lastModified" DESC 
            LIMIT 5
        ''')
        recent_agents = pg_cursor.fetchall()
        
        logger.info("📋 Recent agents in PostgreSQL:")
        for agent in recent_agents:
            logger.info(f"  - {agent[0]} (ID: {agent[1]}, Status: {agent[2]}, Type: {agent[3]}, Location: {agent[4]}, {agent[5]})")
        
        pg_cursor.close()
        pg_conn.close()
        connection.close()
        
        logger.info("✅ Agents RabbitMQ pipeline test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in Step 2: {e}")
        return False

def clear_agents_queue():
    """Clear the agents queue before testing"""
    config = Config()
    
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config.message_queue.rabbitmq_host,
                port=config.message_queue.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    config.message_queue.rabbitmq_user,
                    config.message_queue.rabbitmq_password
                )
            )
        )
        channel = connection.channel()
        
        agents_config = config.tables['agents']
        queue_name = agents_config.queue_name
        
        # Purge the queue
        channel.queue_purge(queue=queue_name)
        logger.info(f"🧹 Cleared queue: {queue_name}")
        
        connection.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error clearing queue: {e}")
        return False

if __name__ == "__main__":
    print("=" * 70)
    print("AGENTS RABBITMQ PIPELINE TEST")
    print("=" * 70)
    
    # Clear the queue first
    print("\n🧹 Clearing agents queue...")
    clear_agents_queue()
    
    # Run the test
    print("\n🚀 Running agents RabbitMQ pipeline test...")
    success = test_agents_rabbitmq_pipeline()
    
    if success:
        print("\n✅ All tests passed! Agents RabbitMQ pipeline is working correctly.")
    else:
        print("\n❌ Some tests failed. Check the logs above.")