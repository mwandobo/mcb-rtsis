#!/usr/bin/env python3
"""
Clear Incoming Fund Transfer data and queue
"""

import psycopg2
import pika
from config import Config
import logging

def clear_incoming_fund_transfer_data():
    """Clear incoming fund transfer data from PostgreSQL"""
    config = Config()
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Get count before deletion
        cursor.execute('SELECT COUNT(*) FROM "incomingFundTransfer"')
        count_before = cursor.fetchone()[0]
        
        # Clear the table
        cursor.execute('DELETE FROM "incomingFundTransfer"')
        
        # Reset the sequence
        cursor.execute('ALTER SEQUENCE "incomingFundTransfer_id_seq" RESTART WITH 1')
        
        conn.commit()
        conn.close()
        
        print(f"Cleared {count_before:,} incoming fund transfer records from PostgreSQL")
        
    except Exception as e:
        print(f"Error clearing incoming fund transfer data: {e}")
        raise

def clear_incoming_fund_transfer_queue():
    """Clear incoming fund transfer queue from RabbitMQ"""
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
        
        # Get message count before purging
        method = channel.queue_declare(queue='incoming_fund_transfer_queue', durable=True, passive=True)
        message_count = method.method.message_count
        
        # Purge the queue
        channel.queue_purge(queue='incoming_fund_transfer_queue')
        
        connection.close()
        
        print(f"Cleared {message_count:,} messages from incoming_fund_transfer_queue")
        
    except Exception as e:
        print(f"Error clearing incoming fund transfer queue: {e}")
        raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clear Incoming Fund Transfer data and queue')
    parser.add_argument('--queue-only', action='store_true', help='Clear only the queue, not PostgreSQL data')
    parser.add_argument('--data-only', action='store_true', help='Clear only PostgreSQL data, not the queue')
    
    args = parser.parse_args()
    
    print("Incoming Fund Transfer Data Cleaner")
    print("=" * 50)
    
    try:
        if args.queue_only:
            print("Clearing queue only...")
            clear_incoming_fund_transfer_queue()
        elif args.data_only:
            print("Clearing PostgreSQL data only...")
            clear_incoming_fund_transfer_data()
        else:
            print("Clearing both PostgreSQL data and RabbitMQ queue...")
            clear_incoming_fund_transfer_data()
            clear_incoming_fund_transfer_queue()
        
        print("Clear operation completed successfully!")
        
    except Exception as e:
        print(f"Clear operation failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()