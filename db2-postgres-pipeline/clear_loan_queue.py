#!/usr/bin/env python3
"""
Clear loan information RabbitMQ queue
"""

import pika
from config import Config

def clear_loan_queue():
    """Clear the loan information queue"""
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
        
        # Purge the queue
        method = channel.queue_purge(queue='loan_information_queue')
        message_count = method.method.message_count
        
        connection.close()
        
        print(f"Cleared {message_count} messages from loan_information_queue")
        
    except Exception as e:
        print(f"Error clearing queue: {e}")

if __name__ == "__main__":
    clear_loan_queue()