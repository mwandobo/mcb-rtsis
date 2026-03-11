#!/usr/bin/env python3
"""
Clear Complaint Statistics RabbitMQ Queue
"""

import pika
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_complaint_statistics_queue():
    """Clear the complaint statistics RabbitMQ queue"""
    
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
        
        # Purge the queue
        queue_name = 'complaint_statistics_queue'
        result = channel.queue_purge(queue_name)
        
        logger.info(f"Queue '{queue_name}' cleared successfully!")
        logger.info(f"Removed {result.method.message_count} messages")
        
        # Also clear dead letter queue if it exists
        try:
            dl_queue_name = 'complaint_statistics_dead_letter'
            dl_result = channel.queue_purge(dl_queue_name)
            logger.info(f"Dead letter queue '{dl_queue_name}' cleared successfully!")
            logger.info(f"Removed {dl_result.method.message_count} messages from dead letter queue")
        except Exception as e:
            logger.warning(f"Could not clear dead letter queue (may not exist): {e}")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Error clearing complaint statistics queue: {e}")
        raise

if __name__ == "__main__":
    clear_complaint_statistics_queue()