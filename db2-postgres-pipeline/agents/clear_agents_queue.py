#!/usr/bin/env python3
"""
Clear Agents RabbitMQ Queue
"""

import pika
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_agents_queue():
    """Clear the agents RabbitMQ queue"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        logger.info("Purging agents_queue...")
        result = channel.queue_purge(queue='agents_queue')
        logger.info(f"Purged {result.method.message_count} messages from agents_queue")
        
        try:
            logger.info("Purging agents_dead_letter queue...")
            result = channel.queue_purge(queue='agents_dead_letter')
            logger.info(f"Purged {result.method.message_count} messages from agents_dead_letter")
        except Exception as e:
            logger.warning(f"Could not purge dead-letter queue: {e}")
        
        connection.close()
        logger.info("Queue clearing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error clearing queue: {e}")
        raise

if __name__ == "__main__":
    clear_agents_queue()
