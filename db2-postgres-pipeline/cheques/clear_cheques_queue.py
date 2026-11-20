#!/usr/bin/env python3
"""
Clear RabbitMQ queue for cheques pipeline
"""

import pika
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_cheques_queue():
    """Clear the cheques RabbitMQ queue"""
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
        
        # Get queue info before clearing
        queue_state = channel.queue_declare(queue='cheques_queue', durable=True, passive=True)
        message_count = queue_state.method.message_count
        
        if message_count > 0:
            print(f"📊 Found {message_count:,} messages in cheques_queue")
            
            # Purge the queue
            channel.queue_purge(queue='cheques_queue')
            print("✅ Cheques queue cleared successfully")
        else:
            print("✅ Cheques queue is already empty")
        
        # Also clear dead letter queue if it exists
        try:
            dlq_state = channel.queue_declare(queue='cheques_dead_letter', durable=True, passive=True)
            dlq_count = dlq_state.method.message_count
            if dlq_count > 0:
                print(f"📊 Found {dlq_count:,} messages in cheques_dead_letter queue")
                channel.queue_purge(queue='cheques_dead_letter')
                print("✅ Cheques dead letter queue cleared")
        except Exception:
            # Dead letter queue doesn't exist, that's fine
            pass
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Error clearing cheques queue: {e}")
        sys.exit(1)

if __name__ == "__main__":
    clear_cheques_queue()