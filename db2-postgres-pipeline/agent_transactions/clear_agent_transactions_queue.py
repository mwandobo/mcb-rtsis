#!/usr/bin/env python3
"""
Clear agent_transactions_queue in RabbitMQ
"""

import pika
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_agent_transactions_queue():
    """Clear the agent_transactions_queue in RabbitMQ"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
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
        
        # Declare queue (in case it doesn't exist)
        queue_info = channel.queue_declare(queue='agent_transactions_queue', durable=True)
        message_count = queue_info.method.message_count
        
        logger.info(f"Agent Transactions queue currently has {message_count:,} messages")
        
        if message_count > 0:
            # Purge the queue
            channel.queue_purge(queue='agent_transactions_queue')
            logger.info("Agent Transactions queue cleared successfully!")
        else:
            logger.info("Agent Transactions queue is already empty")
        
        # Verify queue is empty
        queue_info = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
        final_count = queue_info.method.message_count
        logger.info(f"Final message count: {final_count}")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Error clearing agent_transactions_queue: {e}")
        raise

if __name__ == "__main__":
    clear_agent_transactions_queue()