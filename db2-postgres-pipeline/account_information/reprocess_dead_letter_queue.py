#!/usr/bin/env python3
"""
Reprocess Dead Letter Queue - Move messages from dead-letter back to main queue
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


def reprocess_dead_letter_queue():
    """Move all messages from dead-letter queue back to main queue for reprocessing"""
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
        
        # Check dead-letter queue message count
        dead_letter_queue = channel.queue_declare(queue='account_information_dead_letter', durable=True, passive=True)
        message_count = dead_letter_queue.method.message_count
        
        logger.info(f"Messages in dead-letter queue: {message_count:,}")
        
        if message_count == 0:
            logger.info("Dead-letter queue is empty, nothing to reprocess")
            connection.close()
            return
        
        # Move messages from dead-letter to main queue
        moved_count = 0
        
        def callback(ch, method, properties, body):
            nonlocal moved_count
            try:
                # Publish to main queue
                channel.basic_publish(
                    exchange='',
                    routing_key='account_information_queue',
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                
                # Acknowledge from dead-letter queue
                ch.basic_ack(delivery_tag=method.delivery_tag)
                moved_count += 1
                
                if moved_count % 1000 == 0:
                    logger.info(f"Moved {moved_count:,} messages...")
                
            except Exception as e:
                logger.error(f"Error moving message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        # Consume from dead-letter queue
        channel.basic_qos(prefetch_count=100)
        channel.basic_consume(queue='account_information_dead_letter', on_message_callback=callback)
        
        logger.info("Starting to move messages from dead-letter to main queue...")
        
        # Process all messages
        while moved_count < message_count:
            connection.process_data_events(time_limit=1)
        
        logger.info(f"Successfully moved {moved_count:,} messages from dead-letter to main queue")
        
        # Verify dead-letter queue is empty
        dead_letter_queue = channel.queue_declare(queue='account_information_dead_letter', durable=True, passive=True)
        remaining = dead_letter_queue.method.message_count
        logger.info(f"Messages remaining in dead-letter queue: {remaining:,}")
        
        connection.close()
        logger.info("Reprocessing setup complete - run the pipeline to process these messages")
        
    except Exception as e:
        logger.error(f"Error reprocessing dead-letter queue: {e}")
        raise


if __name__ == "__main__":
    reprocess_dead_letter_queue()
