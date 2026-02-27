#!/usr/bin/env python3
"""
Clear the balances_with_mnos_queue in RabbitMQ
"""

import pika
from config import Config

def clear_queue():
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
        
        # Purge the queue
        result = channel.queue_purge(queue='balances_with_mnos_queue')
        print(f"✓ Cleared {result.method.message_count} messages from balances_with_mnos_queue")
        
        connection.close()
        print("✓ Queue cleared successfully!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise

if __name__ == "__main__":
    clear_queue()
