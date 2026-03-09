#!/usr/bin/env python3
"""
Clear RabbitMQ account_information queue
"""

import pika
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_account_information_queue():
    """Clear the account_information queue in RabbitMQ"""
    
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
        
        # Get queue status before clearing
        queue_state = channel.queue_declare(queue='account_information_queue', durable=True, passive=True)
        message_count_before = queue_state.method.message_count
        
        logger.info(f"Messages in queue before clearing: {message_count_before:,}")
        
        if message_count_before == 0:
            logger.info("Queue is already empty")
            connection.close()
            return
        
        # Purge the queue
        logger.info("Clearing account_information queue...")
        channel.queue_purge(queue='account_information_queue')
        
        # Verify queue is empty
        queue_state = channel.queue_declare(queue='account_information_queue', durable=True, passive=True)
        message_count_after = queue_state.method.message_count
        
        logger.info(f"Messages in queue after clearing: {message_count_after:,}")
        logger.info(f"Cleared {message_count_before:,} messages from account_information queue")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Error clearing account_information queue: {e}")
        raise

if __name__ == "__main__":
    clear_account_information_queue()
