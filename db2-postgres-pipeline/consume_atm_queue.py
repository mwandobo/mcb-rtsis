#!/usr/bin/env python3
"""
ATM Queue Consumer - BOT Project
Consumes ATM records from RabbitMQ and inserts them into PostgreSQL
"""

import pika
import psycopg2
import json
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.atm_processor import AtmProcessor, AtmRecord

class AtmQueueConsumer:
    def __init__(self):
        self.config = Config()
        self.atm_processor = AtmProcessor()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("🏧 ATM Queue Consumer initialized")
    
    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            yield conn
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def process_atm_message(self, ch, method, properties, body):
        """Process individual ATM message"""
        try:
            # Parse message
            record_data = json.loads(body)
            record = AtmRecord(**record_data)
            
            self.logger.info(f"🏧 Processing ATM: {record.atm_code} ({record.atm_name}) | Branch: {record.branch_code} | Status: {record.atm_status}")
            
            # Insert to PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                self.atm_processor.insert_to_postgres(record, cursor)
                conn.commit()
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.info(f"✅ ATM record {record.atm_code} processed successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Error processing ATM message: {e}")
            # Acknowledge to prevent reprocessing
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def start_consuming(self):
        """Start consuming ATM messages from RabbitMQ"""
        try:
            # Setup RabbitMQ connection
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Declare queue (in case it doesn't exist)
            channel.queue_declare(queue='atm_information_queue', durable=True)
            
            # Set up consumer
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue='atm_information_queue',
                on_message_callback=self.process_atm_message
            )
            
            self.logger.info("🔄 Starting ATM queue consumption...")
            self.logger.info("📋 Waiting for ATM messages. To exit press CTRL+C")
            
            # Start consuming
            channel.start_consuming()
            
        except KeyboardInterrupt:
            self.logger.info("🛑 Stopping ATM consumer...")
            channel.stop_consuming()
            connection.close()
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            raise

def main():
    """Main function"""
    print("🏧 ATM Queue Consumer - BOT Project")
    print("=" * 40)
    
    consumer = AtmQueueConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()