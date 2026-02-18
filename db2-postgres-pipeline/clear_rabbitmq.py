#!/usr/bin/env python3
"""
Clear RabbitMQ queue
"""

import pika
from config import Config

def clear_rabbitmq():
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
        
        # Purge the queue (remove all messages)
        result = channel.queue_purge(queue='loan_information_queue')
        print(f"Cleared {result.method.message_count} messages from loan_information_queue")
        
        # Delete and recreate the queue to ensure it's clean
        channel.queue_delete(queue='loan_information_queue')
        print("Deleted loan_information_queue")
        
        channel.queue_declare(queue='loan_information_queue', durable=True)
        print("Recreated loan_information_queue")
        
        connection.close()
        print("RabbitMQ cleared successfully")
        
    except Exception as e:
        print(f"Error clearing RabbitMQ: {e}")

if __name__ == "__main__":
    clear_rabbitmq()