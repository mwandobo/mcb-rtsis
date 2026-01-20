#!/usr/bin/env python3
"""
Clear agent transactions queue in RabbitMQ
"""

import pika
import logging
from config import Config

def clear_agent_transactions_queue():
    """Clear the agent transactions queue"""
    
    config = Config()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        logger.info("🧹 Clearing agent transactions queue...")
        
        # Purge the queue
        try:
            result = channel.queue_purge('agent_transactions_queue')
            logger.info(f"🗑️ Purged {result.method.message_count} messages from agent_transactions_queue")
        except Exception as e:
            logger.warning(f"⚠️ Could not purge agent_transactions_queue: {e}")
        
        # Delete the queue
        try:
            channel.queue_delete('agent_transactions_queue')
            logger.info("🗑️ Deleted agent_transactions_queue")
        except Exception as e:
            logger.warning(f"⚠️ Could not delete agent_transactions_queue: {e}")
        
        connection.close()
        logger.info("✅ Agent transactions queue cleared successfully!")
        
    except Exception as e:
        logger.error(f"❌ Failed to clear queue: {e}")
        raise

if __name__ == "__main__":
    clear_agent_transactions_queue()