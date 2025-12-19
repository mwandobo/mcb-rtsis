#!/usr/bin/env python3
"""
Standalone Agents Pipeline - Simple Test
"""

import sys
import os
import logging
import time
import json
from datetime import datetime

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.agent_processor import AgentProcessor
import pika
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AgentsPipelineStandalone:
    def __init__(self):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.agent_processor = AgentProcessor()
        
        logger.info("👥 Agents Pipeline Standalone initialized")
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for agents"""
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.config.message_queue.rabbitmq_host,
                    port=self.config.message_queue.rabbitmq_port,
                    credentials=pika.PlainCredentials(
                        self.config.message_queue.rabbitmq_user,
                        self.config.message_queue.rabbitmq_password
                    )
                )
            )
            channel = connection.channel()
            
            # Declare agents queue
            channel.queue_declare(queue='agents_queue', durable=True)
            logger.info("✅ Agents queue declared")
            
            connection.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Queue setup error: {e}")
            return False
    
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(
            host=self.config.database.pg_host,
            port=self.config.database.pg_port,
            database=self.config.database.pg_database,
            user=self.config.database.pg_user,
            password=self.config.database.pg_password
        )
    
    def fetch_and_publish_agents(self):
        """Fetch agents data from DB2 and publish to RabbitMQ"""
        agents_config = self.config.tables['agents']
        
        try:
            # Connect to RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.config.message_queue.rabbitmq_host,
                    port=self.config.message_queue.rabbitmq_port,
                    credentials=pika.PlainCredentials(
                        self.config.message_queue.rabbitmq_user,
                        self.config.message_queue.rabbitmq_password
                    )
                )
            )
            channel = connection.channel()
            
            # Connect to DB2 and fetch data
            with self.db2_conn.get_connection() as db2_connection:
                cursor = db2_connection.cursor()
                
                logger.info("🔍 Executing agents query...")
                cursor.execute(agents_config.query)
                
                rows = cursor.fetchall()
                logger.info(f"📊 Found {len(rows)} agent records in DB2")
                
                published_count = 0
                for row in rows:
                    try:
                        # Process the record
                        record = self.agent_processor.process_record(row, 'agents')
                        
                        if self.agent_processor.validate_record(record):
                            # Convert to dict for JSON serialization
                            record_dict = {
                                'source_table': record.source_table,
                                'timestamp_column_value': record.timestamp_column_value,
                                'reporting_date': record.reporting_date,
                                'agent_name': record.agent_name,
                                'agent_id': record.agent_id,
                                'till_number': record.till_number,
                                'business_form': record.business_form,
                                'agent_principal': record.agent_principal,
                                'agent_principal_name': record.agent_principal_name,
                                'gender': record.gender,
                                'registration_date': record.registration_date,
                                'closed_date': record.closed_date,
                                'cert_incorporation': record.cert_incorporation,
                                'nationality': record.nationality,
                                'agent_status': record.agent_status,
                                'agent_type': record.agent_type,
                                'account_number': record.account_number,
                                'region': record.region,
                                'district': record.district,
                                'ward': record.ward,
                                'street': record.street,
                                'house_number': record.house_number,
                                'postal_code': record.postal_code,
                                'country': record.country,
                                'gps_coordinates': record.gps_coordinates,
                                'agent_tax_identification_number': record.agent_tax_identification_number,
                                'business_license': record.business_license,
                                'last_modified': record.last_modified,
                                'original_timestamp': record.original_timestamp,
                                'retry_count': record.retry_count
                            }
                            
                            # Publish to RabbitMQ
                            channel.basic_publish(
                                exchange='',
                                routing_key='agents_queue',
                                body=json.dumps(record_dict, default=str),
                                properties=pika.BasicProperties(
                                    delivery_mode=2,  # Make message persistent
                                )
                            )
                            published_count += 1
                        else:
                            logger.warning(f"⚠️ Invalid agent record: {row[1]}")
                            
                    except Exception as e:
                        logger.error(f"❌ Error processing agent record: {e}")
                
                logger.info(f"📤 Published {published_count} agent records to RabbitMQ")
            
            connection.close()
            return published_count
            
        except Exception as e:
            logger.error(f"❌ Fetch and publish error: {e}")
            return 0
    
    def consume_and_process_agents(self):
        """Consume agents from RabbitMQ and process to PostgreSQL"""
        try:
            # Connect to RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.config.message_queue.rabbitmq_host,
                    port=self.config.message_queue.rabbitmq_port,
                    credentials=pika.PlainCredentials(
                        self.config.message_queue.rabbitmq_user,
                        self.config.message_queue.rabbitmq_password
                    )
                )
            )
            channel = connection.channel()
            
            # Connect to PostgreSQL
            pg_conn = self.get_postgres_connection()
            pg_cursor = pg_conn.cursor()
            
            processed_count = 0
            
            def process_agents_message(ch, method, properties, body):
                nonlocal processed_count
                try:
                    record_data = json.loads(body)
                    
                    # Create AgentRecord from the data
                    from processors.agent_processor import AgentRecord
                    record = AgentRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    self.agent_processor.insert_to_postgres(record, pg_cursor)
                    pg_conn.commit()
                    
                    processed_count += 1
                    logger.info(f"✅ Processed agent: {record.agent_name} (ID: {record.agent_id})")
                    
                    # Acknowledge the message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set up consumer
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='agents_queue', on_message_callback=process_agents_message)
            
            logger.info("🔄 Starting to consume agents messages...")
            
            # Consume messages with timeout
            start_time = time.time()
            timeout = 30  # 30 seconds timeout
            
            while time.time() - start_time < timeout:
                connection.process_data_events(time_limit=1)
                
                # Check if queue is empty
                method_frame, header_frame, body = channel.basic_get(queue='agents_queue', auto_ack=False)
                if method_frame is None:
                    logger.info("📭 Queue is empty, stopping consumer")
                    break
                else:
                    # Put the message back and process it
                    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                    connection.process_data_events(time_limit=1)
            
            logger.info(f"📥 Processed {processed_count} agent records from RabbitMQ")
            
            pg_cursor.close()
            pg_conn.close()
            connection.close()
            
            return processed_count
            
        except Exception as e:
            logger.error(f"❌ Consume and process error: {e}")
            return 0
    
    def verify_postgresql_data(self):
        """Verify data in PostgreSQL"""
        try:
            pg_conn = self.get_postgres_connection()
            pg_cursor = pg_conn.cursor()
            
            # Count total agents
            pg_cursor.execute('SELECT COUNT(*) FROM "agents"')
            total_count = pg_cursor.fetchone()[0]
            logger.info(f"📊 Total agents in PostgreSQL: {total_count}")
            
            # Show recent agents
            pg_cursor.execute('''
                SELECT "agentName", "agentId", "agentStatus", "agentType", "region", "district" 
                FROM "agents" 
                ORDER BY "lastModified" DESC 
                LIMIT 5
            ''')
            recent_agents = pg_cursor.fetchall()
            
            logger.info("📋 Recent agents in PostgreSQL:")
            for agent in recent_agents:
                logger.info(f"  - {agent[0]} (ID: {agent[1]}, Status: {agent[2]}, Type: {agent[3]}, Location: {agent[4]}, {agent[5]})")
            
            pg_cursor.close()
            pg_conn.close()
            
            return total_count
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL verification error: {e}")
            return 0
    
    def run_pipeline_test(self):
        """Run the complete agents pipeline test"""
        logger.info("🚀 Starting Agents Pipeline Standalone Test")
        logger.info("=" * 60)
        
        try:
            # Step 1: Setup RabbitMQ queue
            logger.info("📋 Step 1: Setting up RabbitMQ queue...")
            if not self.setup_rabbitmq_queue():
                return False
            
            # Step 2: Fetch and publish data
            logger.info("📤 Step 2: Fetching agents data from DB2 and publishing to RabbitMQ...")
            published_count = self.fetch_and_publish_agents()
            
            if published_count == 0:
                logger.warning("⚠️ No agents data published")
                return False
            
            # Step 3: Consume and process data
            logger.info("📥 Step 3: Consuming from RabbitMQ and processing to PostgreSQL...")
            processed_count = self.consume_and_process_agents()
            
            # Step 4: Verify data
            logger.info("🔍 Step 4: Verifying data in PostgreSQL...")
            total_count = self.verify_postgresql_data()
            
            # Summary
            logger.info("=" * 60)
            logger.info("📊 PIPELINE TEST SUMMARY:")
            logger.info(f"  - Published to RabbitMQ: {published_count}")
            logger.info(f"  - Processed from RabbitMQ: {processed_count}")
            logger.info(f"  - Total in PostgreSQL: {total_count}")
            logger.info("=" * 60)
            
            if total_count > 0:
                logger.info("✅ Agents pipeline test completed successfully!")
                return True
            else:
                logger.error("❌ No data found in PostgreSQL")
                return False
                
        except Exception as e:
            logger.error(f"❌ Pipeline test error: {e}")
            return False

def main():
    """Main function"""
    print("=" * 70)
    print("AGENTS PIPELINE STANDALONE TEST")
    print("=" * 70)
    
    pipeline = AgentsPipelineStandalone()
    success = pipeline.run_pipeline_test()
    
    if success:
        print("\n✅ All tests passed! Agents pipeline is working correctly.")
    else:
        print("\n❌ Some tests failed. Check the logs above.")

if __name__ == "__main__":
    main()