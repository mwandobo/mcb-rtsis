#!/usr/bin/env python3
"""
Reset queues, delete all PostgreSQL data, and run fresh pipeline
"""

import pika
import psycopg2
import redis
import logging
from config import Config
import subprocess
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def reset_rabbitmq_queues():
    """Delete and recreate all RabbitMQ queues"""
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
        
        # List of all queues
        queues = [
            'cash_information_queue',
            'asset_owned_queue', 
            'balances_bot_queue',
            'balances_with_mnos_queue',
            'balance_with_other_banks_queue',
            'other_assets_queue',
            'overdraft_queue'
        ]
        
        # Delete existing queues
        for queue in queues:
            try:
                channel.queue_delete(queue=queue)
                logger.info(f"🗑️ Deleted queue: {queue}")
            except Exception as e:
                logger.warning(f"Queue {queue} might not exist: {e}")
        
        # Recreate queues
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            logger.info(f"✅ Created queue: {queue}")
        
        connection.close()
        logger.info("🔄 RabbitMQ queues reset successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to reset RabbitMQ queues: {e}")
        raise

def clear_redis_cache():
    """Clear Redis cache"""
    config = Config()
    
    try:
        r = redis.Redis(
            host=config.message_queue.redis_host,
            port=config.message_queue.redis_port,
            db=config.message_queue.redis_db
        )
        r.flushdb()
        logger.info("🗑️ Redis cache cleared")
        
    except Exception as e:
        logger.warning(f"Redis clear failed (might not be running): {e}")

def delete_postgresql_data():
    """Delete all data from PostgreSQL tables"""
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
        
        # List of all tables to clear
        tables = [
            'cash_information',
            'asset_owned',
            'balances_bot',
            'balances_with_mnos',
            'balance_with_other_bank',
            'other_assets',
            'overdraft'
        ]
        
        # Delete data from all tables
        for table in tables:
            try:
                cursor.execute(f'DELETE FROM {table}')
                deleted_count = cursor.rowcount
                logger.info(f"🗑️ Deleted {deleted_count} records from {table}")
            except Exception as e:
                logger.warning(f"Failed to clear {table}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("🗑️ PostgreSQL data cleared successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to clear PostgreSQL data: {e}")
        raise

def run_pipeline():
    """Run the multi-table pipeline"""
    logger.info("🚀 Starting fresh pipeline run...")
    
    try:
        # Run the pipeline
        result = subprocess.run([sys.executable, 'simple_multi_pipeline.py'], 
                              capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logger.info("✅ Pipeline completed successfully")
            logger.info("Pipeline output:")
            print(result.stdout)
        else:
            logger.error("❌ Pipeline failed")
            logger.error("Pipeline errors:")
            print(result.stderr)
            
    except subprocess.TimeoutExpired:
        logger.warning("⏰ Pipeline timed out after 5 minutes")
    except Exception as e:
        logger.error(f"❌ Failed to run pipeline: {e}")

def main():
    """Main reset and run process"""
    logger.info("🔄 Starting complete reset and fresh pipeline run")
    logger.info("=" * 60)
    
    try:
        # Step 1: Reset RabbitMQ queues
        logger.info("Step 1: Resetting RabbitMQ queues...")
        reset_rabbitmq_queues()
        
        # Step 2: Clear Redis cache
        logger.info("Step 2: Clearing Redis cache...")
        clear_redis_cache()
        
        # Step 3: Delete PostgreSQL data
        logger.info("Step 3: Deleting PostgreSQL data...")
        delete_postgresql_data()
        
        # Step 4: Run fresh pipeline
        logger.info("Step 4: Running fresh pipeline...")
        run_pipeline()
        
        logger.info("🎉 Reset and pipeline run completed!")
        
    except Exception as e:
        logger.error(f"❌ Reset and run failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()