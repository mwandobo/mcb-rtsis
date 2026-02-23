#!/usr/bin/env python3
"""
Comprehensive diagnostic for Personal Data Pipeline v3
Checks: RabbitMQ, PostgreSQL, DB2, and pipeline status
"""

import pika
import psycopg2
from config import Config

def check_rabbitmq(config):
    """Check RabbitMQ connection and queue status"""
    print("\n" + "="*60)
    print("🐰 RABBITMQ STATUS")
    print("="*60)
    
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
        
        print(f"✅ Connected to RabbitMQ")
        print(f"   Host: {config.message_queue.rabbitmq_host}:{config.message_queue.rabbitmq_port}")
        
        # Check queue
        try:
            method = channel.queue_declare(queue='personal_data_queue', durable=True, passive=True)
            message_count = method.method.message_count
            consumer_count = method.method.consumer_count
            
            print(f"\n📬 Queue: personal_data_queue")
            print(f"   Messages waiting: {message_count:,}")
            print(f"   Active consumers: {consumer_count}")
            
            if message_count > 0:
                print(f"   ⚠️  Producer is working, but consumer may be slow/stopped")
            elif consumer_count > 0:
                print(f"   ✅ Messages being consumed in real-time")
            else:
                print(f"   ⚠️  Pipeline not running (no consumers)")
                
        except pika.exceptions.ChannelClosedByBroker:
            print(f"   ❌ Queue 'personal_data_queue' does not exist")
            print(f"   Run pipeline to create it")
        
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ RabbitMQ connection failed: {e}")
        return False

def check_postgresql(config):
    """Check PostgreSQL connection and table status"""
    print("\n" + "="*60)
    print("🐘 POSTGRESQL STATUS")
    print("="*60)
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        print(f"✅ Connected to PostgreSQL")
        print(f"   Host: {config.database.pg_host}:{config.database.pg_port}")
        print(f"   Database: {config.database.pg_database}")
        
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'personalData'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f"\n📊 Table: personalData")
            
            # Get count
            cursor.execute('SELECT COUNT(*) FROM "personalData"')
            count = cursor.fetchone()[0]
            print(f"   Total records: {count:,}")
            
            # Get latest record
            cursor.execute('SELECT MAX("reportingDate") FROM "personalData"')
            latest = cursor.fetchone()[0]
            print(f"   Latest record: {latest}")
            
        else:
            print(f"\n❌ Table 'personalData' does not exist")
            print(f"   Run create_personal_data_table.py to create it")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False

def check_db2(config):
    """Check DB2 connection"""
    print("\n" + "="*60)
    print("🗄️  DB2 STATUS")
    print("="*60)
    
    try:
        from db2_connection import DB2Connection
        
        db2_conn = DB2Connection()
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test query
            cursor.execute("SELECT COUNT(*) FROM customer WHERE CUST_TYPE = '1'")
            count = cursor.fetchone()[0]
            
            print(f"✅ Connected to DB2")
            print(f"   Host: {config.database.db2_host}:{config.database.db2_port}")
            print(f"   Database: {config.database.db2_database}")
            print(f"\n📊 Source Data")
            print(f"   Total customers (CUST_TYPE='1'): {count:,}")
        
        return True
        
    except Exception as e:
        print(f"❌ DB2 connection failed: {e}")
        return False

def main():
    """Run all diagnostics"""
    print("\n" + "="*60)
    print("🔍 PERSONAL DATA PIPELINE v3 DIAGNOSTICS")
    print("="*60)
    
    config = Config()
    
    # Run checks
    rabbitmq_ok = check_rabbitmq(config)
    postgresql_ok = check_postgresql(config)
    db2_ok = check_db2(config)
    
    # Summary
    print("\n" + "="*60)
    print("📋 SUMMARY")
    print("="*60)
    print(f"   RabbitMQ:   {'✅ OK' if rabbitmq_ok else '❌ FAILED'}")
    print(f"   PostgreSQL: {'✅ OK' if postgresql_ok else '❌ FAILED'}")
    print(f"   DB2:        {'✅ OK' if db2_ok else '❌ FAILED'}")
    
    if rabbitmq_ok and postgresql_ok and db2_ok:
        print(f"\n✅ All systems operational - ready to run pipeline")
    else:
        print(f"\n⚠️  Some systems have issues - check errors above")
    
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
