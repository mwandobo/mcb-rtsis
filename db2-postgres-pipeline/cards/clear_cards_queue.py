#!/usr/bin/env python3
"""
Clear RabbitMQ queue for cards pipeline
Useful for restarting the pipeline or clearing stuck messages
"""

import pika
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def clear_cards_queue():
    """Clear the cards RabbitMQ queue"""
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
        
        # Purge the main queue
        result = channel.queue_purge(queue='cards_queue')
        print(f"Cleared {result} messages from cards_queue")
        
        # Purge the dead letter queue if it exists
        try:
            result_dlq = channel.queue_purge(queue='cards_dead_letter')
            print(f"Cleared {result_dlq} messages from cards_dead_letter queue")
        except Exception as e:
            print(f"Dead letter queue not found or already empty: {e}")
        
        connection.close()
        print("Queue cleared successfully!")
        
    except Exception as e:
        print(f"Error clearing queue: {e}")
        raise


if __name__ == "__main__":
    clear_cards_queue()
