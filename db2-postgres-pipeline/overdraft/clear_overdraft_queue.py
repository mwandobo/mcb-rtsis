#!/usr/bin/env python3
"""
Clear Overdraft Queue - Purge all messages from the overdraft queue
"""

import pika
import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def clear_overdraft_queue():
    """Clear all messages from the overdraft queue"""
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
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Check queue message count before clearing
        queue = channel.queue_declare(queue='overdraft_queue', durable=True, passive=True)
        message_count = queue.method.message_count
        
        logger.info(f"Messages in queue before clearing: {message_count:,}")
        
        if message_count == 0:
            logger.info("Queue is already empty")
        else:
            # Purge the queue
            channel.queue_purge(queue='overdraft_queue')
            logger.info(f"Cleared {message_count:,} messages from overdraft_queue")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Error clearing overdraft queue: {e}")
        raise


if __name__ == "__main__":
    clear_overdraft_queue()
