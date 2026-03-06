#!/usr/bin/env python3
"""Clear RabbitMQ personal data corporates queue"""
import pika, logging, sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def clear_queue():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    config = Config()
    
    try:
        credentials = pika.PlainCredentials(config.message_queue.rabbitmq_user, config.message_queue.rabbitmq_password)
        parameters = pika.ConnectionParameters(host=config.message_queue.rabbitmq_host, port=config.message_queue.rabbitmq_port, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        queue_state = channel.queue_declare(queue='personal_data_corporates_queue', durable=True, passive=True)
        before = queue_state.method.message_count
        logger.info(f"Messages before: {before:,}")
        
        if before == 0:
            logger.info("Queue already empty")
        else:
            channel.queue_purge(queue='personal_data_corporates_queue')
            logger.info(f"Cleared {before:,} messages")
        
        connection.close()
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    clear_queue()
