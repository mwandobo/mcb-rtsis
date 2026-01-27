#!/usr/bin/env python3
"""
Clear Overdraft Pipeline Data
"""

import pika
import psycopg2
from config import Config

def clear_overdraft_data():
    """Clear both PostgreSQL table and RabbitMQ queue"""
    config = Config()
    
    print("Clearing Overdraft Pipeline Data")
    print("=" * 55)
    
    # Clear PostgreSQL table
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Get current count
        cursor.execute("SELECT COUNT(*) FROM overdraft")
        current_count = cursor.fetchone()[0]
        
        # Clear the table
        cursor.execute("DELETE FROM overdraft")
        conn.commit()
        
        print(f"Clearing PostgreSQL table...")
        print(f"   Current records: {current_count}")
        print(f"   Cleared {current_count} records from overdraft table")
        
        conn.close()
        
    except Exception as e:
        print(f"PostgreSQL error: {e}")
    
    # Clear RabbitMQ queue
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
        
        # Get current message count
        method = channel.queue_declare(queue='overdraft_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        # Purge the queue
        channel.queue_purge(queue='overdraft_queue')
        
        print(f"Clearing RabbitMQ queue...")
        print(f"   Current messages in queue: {message_count}")
        print(f"   Cleared {message_count} messages from overdraft_queue")
        
        connection.close()
        
    except Exception as e:
        print(f"RabbitMQ error: {e}")
    
    print("\nOverdraft cleanup completed")

if __name__ == "__main__":
    clear_overdraft_data()