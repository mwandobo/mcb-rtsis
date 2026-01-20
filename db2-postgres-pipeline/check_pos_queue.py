#!/usr/bin/env python3
"""
Check POS information queue status
"""

import pika
from config import Config

def check_pos_queue():
    """Check the POS information queue status"""
    
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
        
        print("🔍 POS QUEUE STATUS CHECK")
        print("=" * 50)
        
        # Check if queue exists and has messages
        try:
            method = channel.queue_declare(queue='pos_information_queue', durable=True, passive=True)
            queue_count = method.method.message_count
            print(f"📊 Messages in pos_information_queue: {queue_count}")
            
            if queue_count > 0:
                print(f"⚠️ There are {queue_count} stuck messages in the queue!")
                print(f"💡 These messages need to be processed")
            else:
                print(f"✅ Queue is empty - no stuck messages")
                
        except Exception as e:
            print(f"📊 pos_information_queue: Does not exist or error: {e}")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Failed to check queue: {e}")

if __name__ == "__main__":
    check_pos_queue()