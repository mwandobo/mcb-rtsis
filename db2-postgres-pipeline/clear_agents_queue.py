#!/usr/bin/env python3
"""
Clear agents queue specifically
"""

import pika
import logging

def clear_agents_queue():
    """Clear the agents queue in RabbitMQ"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        
        logger.info("🔍 Checking agents queue status...")
        
        # Check queue status
        try:
            method = channel.queue_declare(queue='agents_queue', passive=True)
            message_count = method.method.message_count
            logger.info(f"📊 Agents queue has {message_count} messages")
            
            if message_count > 0:
                # Purge the queue
                logger.info("🗑️ Purging agents queue...")
                channel.queue_purge(queue='agents_queue')
                logger.info(f"✅ Purged {message_count} messages from agents queue")
            else:
                logger.info("✅ Agents queue is already empty")
                
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                logger.info("📋 Agents queue does not exist - will be created when needed")
            else:
                raise
        
        # Also try to delete and recreate the queue for a fresh start
        try:
            logger.info("🗑️ Deleting agents queue for fresh start...")
            channel.queue_delete(queue='agents_queue')
            logger.info("✅ Agents queue deleted")
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                logger.info("📋 Agents queue already doesn't exist")
            else:
                logger.warning(f"⚠️ Could not delete agents queue: {e}")
        
        # Recreate the queue
        logger.info("🏗️ Creating fresh agents queue...")
        channel.queue_declare(queue='agents_queue', durable=True)
        logger.info("✅ Fresh agents queue created")
        
        connection.close()
        logger.info("✅ Agents queue reset completed!")
        
    except Exception as e:
        logger.error(f"❌ Error clearing agents queue: {e}")
        raise

if __name__ == "__main__":
    clear_agents_queue()