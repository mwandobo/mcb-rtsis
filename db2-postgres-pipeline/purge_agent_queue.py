#!/usr/bin/env python3
"""
Purge agent transactions queue completely
"""

import pika
from config import Config

def purge_agent_queue():
    """Purge the agent transactions queue"""
    
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
        
        print("🧹 PURGING AGENT TRANSACTIONS QUEUE")
        print("=" * 50)
        
        # Purge the queue
        try:
            result = channel.queue_purge('agent_transactions_queue')
            print(f"🗑️ Purged {result.method.message_count} messages")
        except Exception as e:
            print(f"⚠️ Could not purge queue: {e}")
        
        # Check final state
        try:
            method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
            final_count = method.method.message_count
            print(f"📊 Final queue count: {final_count}")
        except Exception as e:
            print(f"📊 Queue doesn't exist or is empty")
        
        connection.close()
        print("✅ Queue purge completed")
        
    except Exception as e:
        print(f"❌ Failed to purge queue: {e}")

if __name__ == "__main__":
    purge_agent_queue()