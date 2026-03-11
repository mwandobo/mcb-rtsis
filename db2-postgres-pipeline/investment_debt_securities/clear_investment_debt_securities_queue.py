#!/usr/bin/env python3
"""
Clear Investment Debt Securities RabbitMQ Queue
Utility script to purge the investment debt securities queue
"""

import pika
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_investment_debt_securities_queue():
    """Clear the investment debt securities RabbitMQ queue"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    try:
        # Setup RabbitMQ connection
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
        
        # Check queue status before clearing
        queue_info = channel.queue_declare(queue='investment_debt_securities_queue', durable=True, passive=True)
        message_count = queue_info.method.message_count
        
        logger.info(f"Queue 'investment_debt_securities_queue' currently has {message_count:,} messages")
        
        if message_count > 0:
            # Purge the queue
            purged = channel.queue_purge(queue='investment_debt_securities_queue')
            logger.info(f"Purged {purged.method.message_count:,} messages from 'investment_debt_securities_queue'")
        else:
            logger.info("Queue is already empty")
        
        # Also check and clear dead letter queue if it exists
        try:
            dl_queue_info = channel.queue_declare(queue='investment_debt_securities_dead_letter', durable=True, passive=True)
            dl_message_count = dl_queue_info.method.message_count
            
            logger.info(f"Dead letter queue has {dl_message_count:,} messages")
            
            if dl_message_count > 0:
                dl_purged = channel.queue_purge(queue='investment_debt_securities_dead_letter')
                logger.info(f"Purged {dl_purged.method.message_count:,} messages from dead letter queue")
            
        except Exception as e:
            logger.info(f"Dead letter queue not found or accessible: {e}")
        
        connection.close()
        logger.info("Queue clearing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error clearing queue: {e}")
        raise

if __name__ == "__main__":
    clear_investment_debt_securities_queue()