#!/usr/bin/env python3
"""
Clear Fraud Incidence RabbitMQ Queue
"""

import pika
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def clear_fraud_incidence_queue():
    """Clear the fraud incidence RabbitMQ queue"""

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

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

        queue_name = 'fraud_incidence_queue'
        try:
            result = channel.queue_purge(queue_name)
            logger.info(f"Queue '{queue_name}' cleared successfully!")
            logger.info(f"Removed {result.method.message_count} messages")
        except Exception as e:
            logger.warning(f"Queue '{queue_name}' not found (may not exist yet): {e}")
            connection, channel = pika.BlockingConnection(parameters), None
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

        try:
            dl_queue_name = 'fraud_incidence_dead_letter'
            dl_result = channel.queue_purge(dl_queue_name)
            logger.info(f"Dead letter queue '{dl_queue_name}' cleared successfully!")
            logger.info(f"Removed {dl_result.method.message_count} messages from dead letter queue")
        except Exception as e:
            logger.warning(f"Could not clear dead letter queue (may not exist): {e}")

        connection.close()

    except Exception as e:
        logger.error(f"Error clearing fraud incidence queue: {e}")
        raise

if __name__ == "__main__":
    clear_fraud_incidence_queue()
