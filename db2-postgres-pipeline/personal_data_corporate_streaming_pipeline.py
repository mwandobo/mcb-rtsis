#!/usr/bin/env python3
"""
Personal Data Corporate Streaming Pipeline
Processes corporate customer data from DB2 to PostgreSQL using streaming architecture
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime
from typing import Optional

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.personal_data_corporate_processor import PersonalDataCorporateProcessor
import psycopg2
import pika

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/personal_data_corporate_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PersonalDataCorporateStreamingPipeline:
    """Streaming pipeline for Personal Data Corporate processing"""
    
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size
        self.processor = PersonalDataCorporateProcessor()
        self.queue_name = 'personal_data_corporate_queue'
        self.processed_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        self.config = Config()
        
        # Load SQL query
        self.sql_query = self._load_sql_query()
        
    def _load_sql_query(self) -> str:
        """Load the SQL query from file"""
        sql_file = os.path.join('..', 'sqls', 'personal_data_corporates-v1.sql')
        if not os.path.exists(sql_file):
            sql_file = os.path.join('sqls', 'personal_data_corporates-v1.sql')
        
        try:
            with open(sql_file, 'r') as f:
                query = f.read().strip()
                # Remove the semicolon at the end if present
                if query.endswith(';'):
                    query = query[:-1]
                return query
        except FileNotFoundError:
            logger.error(f"SQL file not found: {sql_file}")
            raise
    
    def setup_rabbitmq(self):
        """Setup RabbitMQ connection and queue"""
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    self.config.message_queue.rabbitmq_user,
                    self.config.message_queue.rabbitmq_password
                )
            ))
            channel = connection.channel()
            
            # Declare queue with durability
            channel.queue_declare(queue=self.queue_name, durable=True)
            
            return connection, channel
        except Exception as e:
            logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def produce_records(self, limit: Optional[int] = None):
        """Producer: Read from DB2 and send to RabbitMQ"""
        logger.info("Starting Personal Data Corporate producer...")
        
        # Setup RabbitMQ
        connection, channel = self.setup_rabbitmq()
        
        try:
            # Connect to DB2
            db2_conn_manager = DB2Connection()
            
            # Build query with cursor-based pagination
            base_query = self.sql_query
            
            # Add ORDER BY and LIMIT for pagination
            paginated_query = f"""
            SELECT * FROM (
                {base_query}
            ) AS corporate_data
            ORDER BY customerIdentificationNumber
            """
            
            if limit:
                paginated_query += f" FETCH FIRST {limit} ROWS ONLY"
            
            logger.info(f"Executing query with limit: {limit}")
            logger.info(f"Query: {paginated_query[:200]}...")
            
            with db2_conn_manager.get_connection() as db2_conn:
                db2_cursor = db2_conn.cursor()
                
                # Execute query
                db2_cursor.execute(paginated_query)
                
                batch = []
                total_sent = 0
                
                while True:
                    # Fetch batch
                    rows = db2_cursor.fetchmany(self.batch_size)
                    if not rows:
                        break
                    
                    # Process each row in the batch
                    for row in rows:
                        try:
                            # Process the record
                            record = self.processor.process_record(row, 'personalDataCorporate')
                            
                            # Convert to JSON for RabbitMQ
                            record_dict = {
                                'reportingDate': record.reportingDate,
                                'companyName': record.companyName,
                                'customerIdentificationNumber': record.customerIdentificationNumber,
                                'establishmentDate': record.establishmentDate.isoformat() if record.establishmentDate else None,
                                'legalForm': record.legalForm,
                                'negativeClientStatus': record.negativeClientStatus,
                                'numberOfEmployees': record.numberOfEmployees,
                                'numberOfEmployeesMale': record.numberOfEmployeesMale,
                                'numberOfEmployeesFemale': record.numberOfEmployeesFemale,
                                'registrationCountry': record.registrationCountry,
                                'registrationNumber': record.registrationNumber,
                                'taxIdentificationNumber': record.taxIdentificationNumber,
                                'tradeName': record.tradeName,
                                'parentName': record.parentName,
                                'parentIncorporationNumber': record.parentIncorporationNumber,
                                'groupId': record.groupId,
                                'sectorSnaClassification': record.sectorSnaClassification,
                                'fullName': record.fullName,
                                'gender': record.gender,
                                'cellPhone': record.cellPhone,
                                'relationType': record.relationType,
                                'nationalId': record.nationalId,
                                'appointmentDate': record.appointmentDate,
                                'terminationDate': record.terminationDate,
                                'rateValueOfSharesOwned': record.rateValueOfSharesOwned,
                                'amountValueOfSharesOwned': record.amountValueOfSharesOwned,
                                'street': record.street,
                                'country': record.country,
                                'region': record.region,
                                'district': record.district,
                                'ward': record.ward,
                                'houseNumber': record.houseNumber,
                                'postalCode': record.postalCode,
                                'poBox': record.poBox,
                                'zipCode': record.zipCode,
                                'primaryPostalCode': record.primaryPostalCode,
                                'primaryRegion': record.primaryRegion,
                                'primaryDistrict': record.primaryDistrict,
                                'primaryWard': record.primaryWard,
                                'primaryStreet': record.primaryStreet,
                                'primaryHouseNumber': record.primaryHouseNumber,
                                'secondaryStreet': record.secondaryStreet,
                                'secondaryHouseNumber': record.secondaryHouseNumber,
                                'secondaryPostalCode': record.secondaryPostalCode,
                                'secondaryRegion': record.secondaryRegion,
                                'secondaryDistrict': record.secondaryDistrict,
                                'secondaryCountry': record.secondaryCountry,
                                'secondaryTextAddress': record.secondaryTextAddress,
                                'mobileNumber': record.mobileNumber,
                                'alternativeMobileNumber': record.alternativeMobileNumber,
                                'fixedLineNumber': record.fixedLineNumber,
                                'faxNumber': record.faxNumber,
                                'emailAddress': record.emailAddress,
                                'socialMedia': record.socialMedia,
                                'entityName': record.entityName,
                                'entityType': record.entityType,
                                'certificateIncorporation': record.certificateIncorporation,
                                'entityRegion': record.entityRegion,
                                'entityDistrict': record.entityDistrict,
                                'entityWard': record.entityWard,
                                'entityStreet': record.entityStreet,
                                'entityHouseNumber': record.entityHouseNumber,
                                'entityPostalCode': record.entityPostalCode,
                                'groupParentCode': record.groupParentCode,
                                'shareOwnedPercentage': record.shareOwnedPercentage,
                                'shareOwnedAmount': record.shareOwnedAmount,
                                'original_timestamp': record.original_timestamp
                            }
                            
                            # Send to RabbitMQ
                            channel.basic_publish(
                                exchange='',
                                routing_key=self.queue_name,
                                body=json.dumps(record_dict),
                                properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
                            )
                            
                            total_sent += 1
                            
                            if total_sent % 50 == 0:
                                logger.info(f"Sent {total_sent} corporate records to queue")
                                
                        except Exception as e:
                            logger.error(f"Error processing record: {e}")
                            self.error_count += 1
                            continue
                    
                    # Small delay between batches
                    time.sleep(0.1)
                
                logger.info(f"Producer finished! Sent {total_sent} corporate records to queue")
            
        except Exception as e:
            logger.error(f"Producer error: {e}")
            raise
        finally:
            connection.close()
    
    def consume_records(self):
        """Consumer: Read from RabbitMQ and insert to PostgreSQL"""
        logger.info("Starting Personal Data Corporate consumer...")
        
        # Setup RabbitMQ
        connection, channel = self.setup_rabbitmq()
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=self.config.database.pg_host,
            port=self.config.database.pg_port,
            database=self.config.database.pg_database,
            user=self.config.database.pg_user,
            password=self.config.database.pg_password
        )
        pg_conn.autocommit = False
        
        def callback(ch, method, properties, body):
            try:
                # Parse the message
                record_data = json.loads(body)
                
                # Create record from dict
                record = self.processor.create_record_from_dict(record_data)
                
                # Insert to PostgreSQL
                pg_cursor = pg_conn.cursor()
                self.processor.insert_to_postgres(record, pg_cursor)
                pg_conn.commit()
                pg_cursor.close()
                
                # Acknowledge the message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
                self.processed_count += 1
                
                if self.processed_count % 25 == 0:
                    elapsed = datetime.now() - self.start_time
                    rate = self.processed_count / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                    logger.info(f"Processed {self.processed_count} corporate records (Rate: {rate:.2f} records/sec)")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Reject and requeue the message
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                self.error_count += 1
        
        # Setup consumer
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        
        logger.info("Waiting for corporate messages. To exit press CTRL+C")
        
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
            channel.stop_consuming()
        finally:
            connection.close()
            pg_conn.close()
    
    def run_streaming_pipeline(self, limit: Optional[int] = None):
        """Run the complete streaming pipeline with producer and consumer"""
        logger.info("Starting Personal Data Corporate Streaming Pipeline...")
        
        # Start consumer in a separate thread
        consumer_thread = threading.Thread(target=self.consume_records)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Give consumer time to start
        time.sleep(2)
        
        # Start producer
        try:
            self.produce_records(limit=limit)
            
            # Wait a bit for consumer to finish processing
            logger.info("Waiting for consumer to finish processing...")
            time.sleep(5)
            
            # Final statistics
            elapsed = datetime.now() - self.start_time
            logger.info(f"""
            Personal Data Corporate Pipeline Summary:
            Records processed: {self.processed_count}
            Errors: {self.error_count}
            Total time: {elapsed}
            Average rate: {self.processed_count / elapsed.total_seconds():.2f} records/sec
            """)
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Personal Data Corporate Streaming Pipeline')
    parser.add_argument('--limit', type=int, help='Limit number of records to process')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = PersonalDataCorporateStreamingPipeline(batch_size=args.batch_size)
    
    try:
        if args.mode == 'producer':
            pipeline.produce_records(limit=args.limit)
        elif args.mode == 'consumer':
            pipeline.consume_records()
        else:  # streaming
            pipeline.run_streaming_pipeline(limit=args.limit)
            
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()