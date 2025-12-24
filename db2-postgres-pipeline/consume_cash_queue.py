#!/usr/bin/env python3
"""
Consume cash messages from RabbitMQ queue
"""

import pika
import psycopg2
import json
import logging
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from processors.cash_processor import CashProcessor, CashRecord

class CashConsumer:
    def __init__(self):
        self.config = Config()
        self.cash_processor = CashProcessor()
        self.processed_count = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("💰 Cash Consumer initialized")
    
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
    
    def process_message(self, ch, method, properties, body):
        """Process a single cash message"""
        try:
            record_data = json.loads(body)
            record = CashRecord(**record_data)
            
            # Insert to PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                self.cash_processor.insert_to_postgres(record, cursor)
                conn.commit()
            
            self.processed_count += 1
            
            if self.processed_count % 100 == 0:
                self.logger.info(f"💰 Processed {self.processed_count} cash records...")
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            self.logger.error(f"❌ Error processing message: {e}")
            # Reject message (don't requeue to avoid infinite loop)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def start_consuming(self):
        """Start consuming messages from cash queue"""
        try:
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
            
            # Set QoS to process one message at a time
            channel.basic_qos(prefetch_count=1)
            
            # Set up consumer
            channel.basic_consume(
                queue='cash_information_queue',
                on_message_callback=self.process_message
            )
            
            self.logger.info("🔄 Starting to consume cash messages...")
            self.logger.info("Press CTRL+C to stop")
            
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                self.logger.info("🛑 Stopping consumer...")
                channel.stop_consuming()
                connection.close()
                
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            raise
        
        self.logger.info(f"✅ Finished processing {self.processed_count} cash records")

def main():
    """Main function"""
    consumer = CashConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()