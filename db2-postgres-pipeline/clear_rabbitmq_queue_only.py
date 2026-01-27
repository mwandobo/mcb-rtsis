#!/usr/bin/env python3
"""
Clear only RabbitMQ queue without touching PostgreSQL data
"""

import pika
import psycopg2
from config import Config

def clear_queue_only():
    """Clear only the RabbitMQ queue, preserve PostgreSQL data"""
    config = Config()
    
    print("Clearing RabbitMQ Queue Only (Preserving PostgreSQL Data)")
    print("=" * 60)
    
    # Check PostgreSQL data first (don't delete)
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM interbankLoanPayable")
        pg_count = cursor.fetchone()[0]
        print(f"PostgreSQL data preserved: {pg_count:,} records in interbankLoanPayable table")
        conn.close()
        
    except Exception as e:
        print(f"PostgreSQL check: {e}")
    
    # Clear RabbitMQ queue only
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
        method = channel.queue_declare(queue='interbank_loan_payable_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        # Purge the queue
        channel.queue_purge(queue='interbank_loan_payable_queue')
        print(f"RabbitMQ queue cleared: {message_count:,} messages removed from interbank_loan_payable_queue")
        
        connection.close()
        
    except Exception as e:
        print(f"RabbitMQ error: {e}")
    
    print("\nQueue reset completed - PostgreSQL data preserved!")

if __name__ == "__main__":
    clear_queue_only()