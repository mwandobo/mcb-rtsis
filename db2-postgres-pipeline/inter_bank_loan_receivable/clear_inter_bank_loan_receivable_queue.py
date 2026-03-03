#!/usr/bin/env python3
"""
Clear inter-bank loan receivable queue in RabbitMQ
"""

import pika
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def clear_inter_bank_loan_receivable_queue():
    """Clear the inter-bank loan receivable queue"""
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
        
        # Declare queue to ensure it exists
        channel.queue_declare(queue='inter_bank_loan_receivable_queue', durable=True)
        
        # Get current message count
        method = channel.queue_declare(queue='inter_bank_loan_receivable_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        print(f"Inter-bank loan receivable queue currently has {message_count} messages")
        
        if message_count > 0:
            # Purge the queue
            channel.queue_purge(queue='inter_bank_loan_receivable_queue')
            print(f"✅ Cleared {message_count} messages from inter_bank_loan_receivable_queue")
        else:
            print("✅ Inter-bank loan receivable queue is already empty")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Error clearing inter-bank loan receivable queue: {e}")
        raise


if __name__ == "__main__":
    clear_inter_bank_loan_receivable_queue()