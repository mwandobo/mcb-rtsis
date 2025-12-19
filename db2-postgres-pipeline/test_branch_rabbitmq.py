#!/usr/bin/env python3
"""
Test Branch Pipeline with RabbitMQ
"""

import pika
import psycopg2
from db2_connection import DB2Connection
import json
import time
import logging
import threading
from datetime import datetime
from contextlib import contextmanager
from dataclasses import asdict

from config import Config
from processors.branch_processor import BranchProcessor, BranchRecord

class BranchPipelineTest:
    def __init__(self):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.running = True
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.branch_processor = BranchProcessor()
        
        self.logger.info("🏢 Branch Pipeline Test initialized")
    
    @contextmanager
    def get_db2_connection(self):
        """Get DB2 connection"""
        with self.db2_conn.get_connection() as conn:
            yield conn
            
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
    
    def setup_branch_queue(self):
        """Setup RabbitMQ queue for branch"""
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
            
            # Declare branch queue
            channel.queue_declare(queue='branch_queue', durable=True)
            
            connection.close()
            self.logger.info("✅ RabbitMQ branch queue created")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def fetch_and_publish_branch(self):
        """Fetch branch data from DB2 and publish to RabbitMQ"""
        branch_config = self.config.tables['branch']
        
        try:
            with self.get_db2_connection() as db2_conn:
                cursor = db2_conn.cursor()
                self.logger.info("📊 Executing branch query...")
                cursor.execute(branch_config.query)
                
                records = []
                row_count = 0
                for row in cursor.fetchall():
                    row_count += 1
                    try:
                        record = self.branch_processor.process_record(row, 'branch')
                        if self.branch_processor.validate_record(record):
                            records.append(record)
                        else:
                            self.logger.warning(f"⚠️ Invalid branch record: {record.branch_code}")
                    except Exception as e:
                        self.logger.error(f"❌ Error processing row {row_count}: {e}")
                
                self.logger.info(f"📊 Processed {row_count} rows, {len(records)} valid records")
                
                if records:
                    self.publish_to_queue(records, 'branch_queue')
                    self.logger.info(f"🏢 Published {len(records)} branch records to queue")
                    return len(records)
                else:
                    self.logger.info("🏢 No branch records to publish")
                    return 0
                    
        except Exception as e:
            self.logger.error(f"❌ Branch fetch error: {e}")
            raise
    
    def publish_to_queue(self, records, queue_name):
        """Publish records to RabbitMQ queue"""
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
            
            published_count = 0
            for record in records:
                message = json.dumps(asdict(record), default=str)
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                published_count += 1
            
            connection.close()
            self.logger.info(f"📤 Published {published_count} messages to {queue_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Publish error to {queue_name}: {e}")
            raise
    
    def consume_branch_queue(self):
        """Consume branch records from queue and insert to PostgreSQL"""
        self.logger.info("🔄 Starting branch consumer...")
        
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
            
            processed_count = 0
            
            def process_branch_message(ch, method, properties, body):
                nonlocal processed_count
                try:
                    record_data = json.loads(body)
                    record = BranchRecord(**record_data)
                    
                    with self.get_postgres_connection() as pg_conn:
                        cursor = pg_conn.cursor()
                        self.branch_processor.insert_to_postgres(record, cursor)
                        pg_conn.commit()
                        cursor.close()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    processed_count += 1
                    self.logger.info(f"✅ Processed branch: {record.branch_code} - {record.branch_name}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Branch processing error: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_consume(
                queue='branch_queue',
                on_message_callback=process_branch_message
            )
            
            # Process messages for a limited time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            self.logger.info(f"🏁 Branch consumer finished. Processed {processed_count} records")
            return processed_count
            
        except Exception as e:
            self.logger.error(f"❌ Branch consumer error: {e}")
            raise
    
    def check_queue_status(self):
        """Check RabbitMQ queue status"""
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
            
            # Check queue status
            method = channel.queue_declare(queue='branch_queue', durable=True, passive=True)
            message_count = method.method.message_count
            
            connection.close()
            self.logger.info(f"📊 Queue 'branch_queue' has {message_count} messages")
            return message_count
            
        except Exception as e:
            self.logger.error(f"❌ Queue status check error: {e}")
            return -1
    
    def run_test(self):
        """Run complete branch pipeline test"""
        self.logger.info("🚀 Starting Branch Pipeline Test with RabbitMQ")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Setup queue
            self.logger.info("🔧 Step 1: Setting up RabbitMQ queue...")
            self.setup_branch_queue()
            
            # Step 2: Check initial queue status
            self.logger.info("📊 Step 2: Checking initial queue status...")
            initial_count = self.check_queue_status()
            
            # Step 3: Fetch and publish data
            self.logger.info("📤 Step 3: Fetching branch data from DB2 and publishing to queue...")
            published_count = self.fetch_and_publish_branch()
            
            # Step 4: Check queue after publishing
            self.logger.info("📊 Step 4: Checking queue after publishing...")
            after_publish_count = self.check_queue_status()
            
            # Step 5: Start consumer
            self.logger.info("🔄 Step 5: Starting consumer to process messages...")
            processed_count = self.consume_branch_queue()
            
            # Step 6: Final queue status
            self.logger.info("📊 Step 6: Checking final queue status...")
            final_count = self.check_queue_status()
            
            # Step 7: Verify PostgreSQL data
            self.logger.info("💾 Step 7: Verifying data in PostgreSQL...")
            with self.get_postgres_connection() as pg_conn:
                cursor = pg_conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM "branch"')
                pg_count = cursor.fetchone()[0]
                cursor.close()
            
            # Summary
            self.logger.info("=" * 60)
            self.logger.info("📋 BRANCH PIPELINE TEST SUMMARY")
            self.logger.info("=" * 60)
            self.logger.info(f"📤 Records published to queue: {published_count}")
            self.logger.info(f"🔄 Records processed from queue: {processed_count}")
            self.logger.info(f"💾 Total records in PostgreSQL: {pg_count}")
            self.logger.info(f"📊 Final queue message count: {final_count}")
            
            if published_count > 0 and processed_count > 0:
                self.logger.info("✅ Branch pipeline test PASSED!")
                return True
            else:
                self.logger.error("❌ Branch pipeline test FAILED!")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Branch pipeline test error: {e}")
            return False

if __name__ == "__main__":
    test = BranchPipelineTest()
    success = test.run_test()
    
    if success:
        print("\n🎉 Branch pipeline with RabbitMQ is working correctly!")
    else:
        print("\n💥 Branch pipeline test failed!")
        exit(1)