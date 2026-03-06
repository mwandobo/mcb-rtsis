#!/usr/bin/env python3
"""
Clear deposits RabbitMQ queue
"""

import pika
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def clear_deposits_queue():
    """Clear the deposits queue"""
    config = Config()
    
    try:
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
        
        # Declare queue to ensure it exists
        result = channel.queue_declare(queue='deposits_queue', durable=True, passive=True)
        messages_before = result.method.message_count
        logger.info(f"Messages before: {messages_before}")
        
        if messages_before > 0:
            # Purge the queue
            channel.queue_purge(queue='deposits_queue')
            logger.info(f"Cleared {messages_before} messages from deposits_queue")
        else:
            logger.info("Queue already empty")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Error clearing deposits queue: {e}")
        raise


if __name__ == "__main__":
    clear_deposits_queue()
