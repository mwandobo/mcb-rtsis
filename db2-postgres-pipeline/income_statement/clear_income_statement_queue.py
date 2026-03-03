#!/usr/bin/env python3
"""
Clear income statement queue in RabbitMQ
"""

import pika
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def clear_income_statement_queue():
    """Clear the income statement queue"""
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
        channel.queue_declare(queue='income_statement_queue', durable=True)
        
        # Get current message count
        method = channel.queue_declare(queue='income_statement_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        print(f"Income statement queue currently has {message_count} messages")
        
        if message_count > 0:
            # Purge the queue
            channel.queue_purge(queue='income_statement_queue')
            print(f"✅ Cleared {message_count} messages from income_statement_queue")
        else:
            print("✅ Income statement queue is already empty")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Error clearing income statement queue: {e}")
        raise


if __name__ == "__main__":
    clear_income_statement_queue()