#!/usr/bin/env python3
"""
Clear the inter_bank_loan_receivable_queue in RabbitMQ
"""

import pika
import logging
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clear_queue():
    """Clear the inter_bank_loan_receivable_queue"""
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
        
        logging.info("Connected to RabbitMQ")
        
        # Declare queue (in case it doesn't exist)
        channel.queue_declare(queue='inter_bank_loan_receivable_queue', durable=True)
        
        # Purge the queue
        result = channel.queue_purge(queue='inter_bank_loan_receivable_queue')
        logging.info(f"Purged {result.method.message_count} messages from inter_bank_loan_receivable_queue")
        
        connection.close()
        logging.info("✅ Queue cleared successfully!")
        
    except Exception as e:
        logging.error(f"Error clearing queue: {e}")
        raise

if __name__ == "__main__":
    clear_queue()
