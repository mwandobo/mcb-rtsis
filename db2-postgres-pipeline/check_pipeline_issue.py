#!/usr/bin/env python3
"""
Check why pipeline stops at 9 records instead of 10
"""

import psycopg2
import pika
import json
from config import Config

def check_pipeline_issue():
    """Check for pipeline processing issues"""
    
    config = Config()
    
    print("🔍 PIPELINE ISSUE INVESTIGATION")
    print("=" * 50)
    
    # 1. Check PostgreSQL table
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM "agentTransactions"')
        pg_count = cursor.fetchone()[0]
        print(f"📊 PostgreSQL records: {pg_count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ PostgreSQL check failed: {e}")
        pg_count = 0
    
    # 2. Check RabbitMQ queue
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
        
        # Check if queue exists and has messages
        try:
            method = channel.queue_declare(queue='agent_transactions_queue', durable=True, passive=True)
            queue_count = method.method.message_count
            print(f"📊 RabbitMQ queue messages: {queue_count}")
        except Exception as e:
            print(f"📊 RabbitMQ queue messages: 0 (queue doesn't exist)")
            queue_count = 0
        
        connection.close()
        
    except Exception as e:
        print(f"❌ RabbitMQ check failed: {e}")
        queue_count = 0
    
    # 3. Analysis
    print(f"\n🔍 ANALYSIS:")
    print(f"   Expected: 10 records (batch size)")
    print(f"   PostgreSQL: {pg_count} records")
    print(f"   RabbitMQ: {queue_count} pending messages")
    
    if pg_count == 9 and queue_count == 0:
        print(f"\n💡 LIKELY EXPLANATION:")
        print(f"   The source DB2 query only returned 9 matching records")
        print(f"   This means there are only 9 agent transactions that meet")
        print(f"   the criteria in the source database for the date range")
        print(f"   and GL account filters.")
        
    elif pg_count < 10 and queue_count > 0:
        print(f"\n⚠️ PROCESSING ISSUE:")
        print(f"   There are {queue_count} messages stuck in RabbitMQ")
        print(f"   These messages failed to process into PostgreSQL")
        
    elif pg_count == 10:
        print(f"\n✅ NO ISSUE:")
        print(f"   All 10 records processed successfully")
        
    else:
        print(f"\n❓ UNCLEAR SITUATION:")
        print(f"   Need to investigate further")
    
    return pg_count, queue_count

if __name__ == "__main__":
    check_pipeline_issue()