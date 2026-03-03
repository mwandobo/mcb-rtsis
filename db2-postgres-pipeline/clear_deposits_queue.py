#!/usr/bin/env python3
"""
Clear deposits queue
"""

import pika
from config import Config

def clear_deposits_queue():
    """Clear the deposits queue"""
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
        
        # Get queue info
        method = channel.queue_declare(queue='deposits_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        print(f"📊 Current queue size: {message_count:,} messages")
        
        if message_count > 0:
            # Purge the queue
            channel.queue_purge(queue='deposits_queue')
            print(f"🗑️ Cleared {message_count:,} messages from deposits_queue")
        else:
            print("✅ Queue is already empty")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Failed to clear queue: {e}")

if __name__ == "__main__":
    clear_deposits_queue()