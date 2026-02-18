#!/usr/bin/env python3
"""
Clear personal_data_corporate_queue in RabbitMQ
"""

import pika
import logging
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_queue():
    config = Config()
    
    logger.info("="*80)
    logger.info("CLEARING PERSONAL DATA CORPORATE QUEUE")
    logger.info("="*80)
    
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
        
        # Purge queue
        result = channel.queue_purge('personal_data_corporate_queue')
        logger.info(f"✓ Cleared {result} messages from personal_data_corporate_queue")
        
        connection.close()
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    clear_queue()
