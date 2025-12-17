#!/usr/bin/env python3
"""
Clear all RabbitMQ messages and queues
"""

import pika
import logging
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_rabbitmq():
    """Clear all RabbitMQ queues and messages"""
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
        
        # List of all queues to clear
        queues = [
            'cash_information_queue',
            'asset_owned_queue', 
            'balances_bot_queue',
            'balances_with_mnos_queue',
            'balance_with_other_banks_queue',
            'other_assets_queue',
            'overdraft_queue'
        ]
        
        logger.info("🧹 Starting RabbitMQ cleanup...")
        
        # Purge all messages from each queue
        for queue in queues:
            try:
                method = channel.queue_purge(queue=queue)
                message_count = method.method.message_count
                logger.info(f"🗑️ Purged {message_count} messages from {queue}")
            except Exception as e:
                logger.warning(f"⚠️ Could not purge {queue}: {e}")
        
        # Delete all queues
        for queue in queues:
            try:
                channel.queue_delete(queue=queue)
                logger.info(f"🗑️ Deleted queue: {queue}")
            except Exception as e:
                logger.warning(f"⚠️ Could not delete {queue}: {e}")
        
        connection.close()
        logger.info("✅ RabbitMQ cleanup completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Failed to clear RabbitMQ: {e}")
        raise

if __name__ == "__main__":
    clear_rabbitmq()