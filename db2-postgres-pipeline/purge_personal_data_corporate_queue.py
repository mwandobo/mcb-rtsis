import pika
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def purge_queue():
    """Purge the personal_data_corporate_queue"""
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Declare queue (in case it doesn't exist)
        channel.queue_declare(queue='personal_data_corporate_queue', durable=True)
        
        # Purge the queue
        result = channel.queue_purge(queue='personal_data_corporate_queue')
        logging.info(f"Purged {result.method.message_count} messages from personal_data_corporate_queue")
        
        connection.close()
        logging.info("Queue purged successfully")
        
    except Exception as e:
        logging.error(f"Error purging queue: {e}")
        raise

if __name__ == "__main__":
    purge_queue()
