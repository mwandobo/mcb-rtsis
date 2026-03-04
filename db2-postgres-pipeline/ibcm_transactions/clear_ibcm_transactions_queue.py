#!/usr/bin/env python3
"""
Clear the ibcm_transactions_queue and ibcm_transactions_dead_letter queues in RabbitMQ.
"""

import pika
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_queue(queue_name):
    """Clear a specific queue"""
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
        result = channel.queue_purge(queue=queue_name)
        print(f"Cleared {result.method.message_count} messages from queue '{queue_name}'")
        
        connection.close()
        
    except Exception as e:
        print(f"Error clearing queue '{queue_name}': {e}")

def main():
    clear_queue('ibcm_transactions_queue')
    clear_queue('ibcm_transactions_dead_letter')

if __name__ == "__main__":
    main()