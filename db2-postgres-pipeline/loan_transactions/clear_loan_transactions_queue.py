#!/usr/bin/env python3
"""
Clear RabbitMQ loan transactions queue
"""

import pika
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_loan_transactions_queue():
    """Clear the loan transactions queue in RabbitMQ"""
    
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
        queue_state = channel.queue_declare(queue='loan_transactions_queue', durable=True, passive=True)
        message_count_before = queue_state.method.message_count
        
        logger.info(f"Messages in queue before clearing: {message_count_before:,}")
        
        if message_count_before == 0:
            logger.info("Queue is already empty")
            connection.close()
            return
        
        # Purge the queue
        logger.info("Clearing loan transactions queue...")
        channel.queue_purge(queue='loan_transactions_queue')
        
        # Verify queue is empty
        queue_state = channel.queue_declare(queue='loan_transactions_queue', durable=True, passive=True)
        message_count_after = queue_state.method.message_count
        
        logger.info(f"Messages in queue after clearing: {message_count_after:,}")
        logger.info(f"Cleared {message_count_before:,} messages from loan transactions queue")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Error clearing loan transactions queue: {e}")
        raise

if __name__ == "__main__":
    clear_loan_transactions_queue()
