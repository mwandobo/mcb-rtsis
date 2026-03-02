#!/usr/bin/env python3
"""
Clear balances_bot_queue in RabbitMQ
"""

import pika
import logging
from config import Config

def clear_balances_bot_queue():
    """Clear the balances_bot_queue"""
    config = Config()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        # Get queue info
        method = channel.queue_declare(queue='balances_bot_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        logger.info(f"Queue 'balances_bot_queue' has {message_count:,} messages")
        
        if message_count > 0:
            # Purge the queue
            channel.queue_purge(queue='balances_bot_queue')
            logger.info(f"Cleared {message_count:,} messages from 'balances_bot_queue'")
        else:
            logger.info("Queue is already empty")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Error clearing queue: {e}")
        raise

if __name__ == "__main__":
    clear_balances_bot_queue()
