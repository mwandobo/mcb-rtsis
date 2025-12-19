#!/usr/bin/env python3
"""
Check Branch Queue Status in RabbitMQ
"""

import pika
import logging
from config import Config

def check_branch_queue():
    """Check if branch queue exists and its status"""
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
        
        # Check if branch queue exists
        try:
            method = channel.queue_declare(queue='branch_queue', durable=True, passive=True)
            message_count = method.method.message_count
            consumer_count = method.method.consumer_count
            
            print("✅ Branch Queue Status:")
            print(f"   Queue Name: branch_queue")
            print(f"   Messages: {message_count}")
            print(f"   Consumers: {consumer_count}")
            print(f"   Status: EXISTS")
            
        except Exception as e:
            print(f"❌ Branch queue does not exist: {e}")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ RabbitMQ connection error: {e}")

if __name__ == "__main__":
    print("🔍 Checking Branch Queue in RabbitMQ...")
    check_branch_queue()