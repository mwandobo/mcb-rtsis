#!/usr/bin/env python3
"""
Check RabbitMQ status for personal_data_queue
"""

import pika
from config import Config

def main():
    config = Config()
    
    print("\n" + "="*60)
    print("🐰 RABBITMQ PERSONAL DATA QUEUE STATUS CHECK")
    print("="*60)
    
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
        
        print(f"\n📡 Connecting to RabbitMQ...")
        print(f"   Host: {config.message_queue.rabbitmq_host}")
        print(f"   Port: {config.message_queue.rabbitmq_port}")
        print(f"   User: {config.message_queue.rabbitmq_user}")
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        print("✅ Connected to RabbitMQ successfully!")
        
        # Check queue status
        print(f"\n📊 Checking 'personal_data_queue' status...")
        method = channel.queue_declare(queue='personal_data_queue', durable=True, passive=True)
        
        message_count = method.method.message_count
        consumer_count = method.method.consumer_count
        
        print(f"\n{'='*60}")
        print(f"📬 Queue: personal_data_queue")
        print(f"{'='*60}")
        print(f"   Messages in queue: {message_count:,}")
        print(f"   Active consumers: {consumer_count}")
        print(f"   Status: {'✅ Queue exists' if method else '❌ Queue not found'}")
        print(f"{'='*60}")
        
        if message_count > 0:
            print(f"\n⚠️  There are {message_count:,} messages waiting to be consumed")
            print(f"   This means the producer is working but consumer may be slow or stopped")
        elif consumer_count > 0:
            print(f"\n✅ Queue is empty with {consumer_count} active consumer(s)")
            print(f"   This means messages are being consumed as they arrive")
        else:
            print(f"\n⚠️  Queue is empty with no active consumers")
            print(f"   Pipeline may not be running")
        
        connection.close()
        
    except pika.exceptions.ChannelClosedByBroker as e:
        print(f"\n❌ Queue does not exist: {e}")
        print(f"   Run the pipeline to create the queue")
    except Exception as e:
        print(f"\n❌ Error connecting to RabbitMQ: {e}")
        print(f"   Make sure RabbitMQ is running")

if __name__ == "__main__":
    main()
