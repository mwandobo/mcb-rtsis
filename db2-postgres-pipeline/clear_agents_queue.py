#!/usr/bin/env python3
"""
Clear agents RabbitMQ queue and PostgreSQL table
"""

import pika
import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_agents():
    """Clear agents queue and table"""
    config = Config()
    
    try:
        # Clear RabbitMQ queue
        logger.info("Clearing RabbitMQ queue 'agents_queue'...")
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
        
        # Purge the queue
        channel.queue_purge('agents_queue')
        logger.info("✓ RabbitMQ queue 'agents_queue' cleared")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Failed to clear RabbitMQ queue: {e}")
    
    try:
        # Clear PostgreSQL table
        logger.info("Clearing PostgreSQL 'agents' table...")
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Get count before deletion
        cursor.execute('SELECT COUNT(*) FROM "agents"')
        count_before = cursor.fetchone()[0]
        logger.info(f"Records in agents table: {count_before:,}")
        
        # Truncate table
        cursor.execute('TRUNCATE TABLE "agents" CASCADE')
        conn.commit()
        
        # Verify
        cursor.execute('SELECT COUNT(*) FROM "agents"')
        count_after = cursor.fetchone()[0]
        
        logger.info(f"✓ PostgreSQL 'agents' table cleared (deleted {count_before:,} records)")
        logger.info(f"Current count: {count_after}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to clear PostgreSQL table: {e}")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("CLEARING AGENTS QUEUE AND TABLE")
    logger.info("=" * 60)
    clear_agents()
    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)
