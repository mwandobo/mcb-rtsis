#!/usr/bin/env python3
"""
Consume remaining agents messages from RabbitMQ
"""

import pika
import json
import psycopg2
import logging
from config import Config
from processors.agent_processor import AgentProcessor

def consume_remaining_agents():
    """Consume and process remaining agents messages from RabbitMQ"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔄 CONSUMING REMAINING AGENTS MESSAGES")
    logger.info("=" * 50)
    
    # Initialize processor
    processor = AgentProcessor(config)
    
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Ensure queue exists
    channel.queue_declare(queue='agents_queue', durable=True)
    
    # Check initial queue status
    method = channel.queue_declare(queue='agents_queue', passive=True)
    initial_count = method.method.message_count
    logger.info(f"📊 Initial messages in queue: {initial_count}")
    
    processed_count = 0
    
    def callback(ch, method, properties, body):
        nonlocal processed_count
        try:
            # Parse message
            agent_data = json.loads(body)
            
            # Process the agent
            success = processor.process_agent(agent_data)
            
            if success:
                processed_count += 1
                agent_name = agent_data.get('agentName', 'Unknown')
                agent_id = agent_data.get('agentId', 'None')
                logger.info(f"✅ Processed agent: {agent_name} (ID: {agent_id})")
                
                # Acknowledge the message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
                # Log progress every 50 messages
                if processed_count % 50 == 0:
                    logger.info(f"📊 Progress: {processed_count} agents processed")
            else:
                logger.error(f"❌ Failed to process agent: {agent_data.get('agentName', 'Unknown')}")
                # Reject and requeue the message
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            # Reject and requeue the message
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    # Set up consumer
    channel.basic_qos(prefetch_count=1)  # Process one message at a time
    channel.basic_consume(queue='agents_queue', on_message_callback=callback)
    
    logger.info("🔄 Starting to consume remaining agents messages...")
    
    try:
        # Start consuming with a timeout mechanism
        timeout_counter = 0
        max_timeout = 30  # 30 seconds of no messages before stopping
        
        while True:
            # Check if queue is empty
            method = channel.queue_declare(queue='agents_queue', passive=True)
            current_count = method.method.message_count
            
            if current_count == 0:
                logger.info("✅ All messages processed - queue is empty")
                break
            
            # Process messages for a short time
            connection.process_data_events(time_limit=1)
            
            # Check if we're making progress
            new_method = channel.queue_declare(queue='agents_queue', passive=True)
            new_count = new_method.method.message_count
            
            if new_count == current_count:
                timeout_counter += 1
                if timeout_counter >= max_timeout:
                    logger.warning(f"⚠️ No progress for {max_timeout} seconds - stopping consumer")
                    break
            else:
                timeout_counter = 0
                
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    
    # Final status
    method = channel.queue_declare(queue='agents_queue', passive=True)
    final_count = method.method.message_count
    
    logger.info("=" * 50)
    logger.info(f"📊 CONSUMPTION SUMMARY:")
    logger.info(f"   - Initial messages: {initial_count}")
    logger.info(f"   - Processed messages: {processed_count}")
    logger.info(f"   - Remaining messages: {final_count}")
    logger.info(f"   - Messages consumed: {initial_count - final_count}")
    
    connection.close()
    
    # Check PostgreSQL count
    try:
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('SELECT COUNT(*) FROM agents')
        total_agents = pg_cursor.fetchone()[0]
        logger.info(f"📊 Total agents in PostgreSQL: {total_agents}")
        pg_conn.close()
    except Exception as e:
        logger.error(f"❌ Error checking PostgreSQL: {e}")
    
    logger.info("✅ Remaining agents consumption completed!")

if __name__ == "__main__":
    consume_remaining_agents()