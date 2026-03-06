#!/usr/bin/env python3
"""
Clear Balance with BOT RabbitMQ Queue
"""

import pika
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_queue():
    """Clear the balance with BOT queue"""
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
        
        queue_name = 'balance_with_bot_queue'
        result = channel.queue_purge(queue_name)
        
        print(f"✅ Cleared {result.method.message_count} messages from '{queue_name}'")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Error clearing queue: {e}")
        sys.exit(1)

if __name__ == "__main__":
    clear_queue()
