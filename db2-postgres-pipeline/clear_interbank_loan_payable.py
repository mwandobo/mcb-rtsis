#!/usr/bin/env python3
"""
Clear Interbank Loan Payable table and queue
"""

import psycopg2
import pika
from config import Config

def clear_interbank_loan_payable():
    """Clear the interbankLoanPayable table and RabbitMQ queue"""
    
    print("Clearing Interbank Loan Payable Pipeline Data")
    print("=" * 55)
    
    try:
        # Clear PostgreSQL table
        print("Clearing PostgreSQL table...")
        config = Config()
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        pg_cursor = pg_conn.cursor()
        
        # Get current count
        pg_cursor.execute('SELECT COUNT(*) FROM "interbankLoanPayable"')
        current_count = pg_cursor.fetchone()[0]
        print(f"   Current records: {current_count}")
        
        # Clear the table
        pg_cursor.execute('DELETE FROM "interbankLoanPayable"')
        pg_conn.commit()
        
        print(f"   Cleared {current_count} records from interbankLoanPayable table")
        
        pg_cursor.close()
        pg_conn.close()
        
    except Exception as e:
        print(f"   Error clearing PostgreSQL: {e}")
    
    try:
        # Clear RabbitMQ queue
        print("Clearing RabbitMQ queue...")
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=config.message_queue.rabbitmq_host,
            port=config.message_queue.rabbitmq_port,
            credentials=pika.PlainCredentials(
                config.message_queue.rabbitmq_user,
                config.message_queue.rabbitmq_password
            )
        ))
        channel = connection.channel()
        
        queue_name = 'interbank_loan_payable_queue'
        
        # Get queue info
        method = channel.queue_declare(queue=queue_name, durable=True, passive=True)
        message_count = method.method.message_count
        print(f"   Current messages in queue: {message_count}")
        
        # Purge the queue
        channel.queue_purge(queue=queue_name)
        print(f"   Cleared {message_count} messages from {queue_name}")
        
        connection.close()
        
    except Exception as e:
        print(f"   Error clearing RabbitMQ: {e}")
    
    print("\nInterbank Loan Payable cleanup completed")

if __name__ == "__main__":
    clear_interbank_loan_payable()