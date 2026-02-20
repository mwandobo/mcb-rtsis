#!/usr/bin/env python3
"""
Clear personal data table and RabbitMQ queue
"""

import pika
import psycopg2
from config import Config

def main():
    config = Config()
    
    print("\n" + "="*60)
    print("🧹 CLEARING PERSONAL DATA")
    print("="*60)
    
    # 1. Clear RabbitMQ queue
    print("\n1️⃣ Clearing RabbitMQ queue...")
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
        
        # Check queue
        method = channel.queue_declare(queue='personal_data_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        if message_count > 0:
            print(f"   Purging {message_count:,} messages...")
            channel.queue_purge('personal_data_queue')
            print(f"   ✅ Queue purged")
        else:
            print(f"   ✅ Queue already empty")
        
        connection.close()
        
    except Exception as e:
        print(f"   ⚠️  Queue error: {e}")
    
    # 2. Clear PostgreSQL table
    print("\n2️⃣ Clearing PostgreSQL table...")
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
        cursor.execute('SELECT COUNT(*) FROM "personalData"')
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"   Deleting {count:,} records...")
            cursor.execute('DELETE FROM "personalData"')
            conn.commit()
            print(f"   ✅ Table cleared")
        else:
            print(f"   ✅ Table already empty")
        
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Database error: {e}")
    
    print("\n" + "="*60)
    print("✅ CLEANUP COMPLETE")
    print("="*60)
    print("Ready to run: python run_personal_data_pipeline.py")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
