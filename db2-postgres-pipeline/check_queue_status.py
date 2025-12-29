#!/usr/bin/env python3
"""
Check RabbitMQ queue status
"""

import pika
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_queue_status():
    """Check how many messages are in the queue"""
    
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
        
        # Check cash queue
        method = channel.queue_declare(queue='cash_information_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        logger.info(f"💰 Cash queue status: {message_count:,} messages waiting")
        
        connection.close()
        
        return message_count
        
    except Exception as e:
        logger.error(f"❌ Failed to check queue: {e}")
        return 0

if __name__ == "__main__":
    check_queue_status()