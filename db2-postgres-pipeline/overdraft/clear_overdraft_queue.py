#!/usr/bin/env python3
"""
Clear overdraft queue in RabbitMQ
"""

import sys
import os
import pika

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def clear_overdraft_queue():
    """Clear the overdraft queue"""
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
        
        # Get queue info before clearing
        method = channel.queue_declare(queue='overdraft_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        print(f"Overdraft queue currently has {message_count:,} messages")
        
        if message_count > 0:
            # Purge the queue
            channel.queue_purge(queue='overdraft_queue')
            print(f"✅ Cleared {message_count:,} messages from overdraft_queue")
        else:
            print("✅ Overdraft queue is already empty")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Error clearing overdraft queue: {e}")
        raise


if __name__ == "__main__":
    clear_overdraft_queue()