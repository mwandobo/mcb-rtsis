#!/usr/bin/env python3
"""
Investment Debt Securities Streaming Pipeline - Producer and Consumer run simultaneously
"""

import pika
import psycopg2
import json
import logging
import threading
import time
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from db2_connection import DB2Connection
from processors.investment_debt_securities_processor import InvestmentDebtSecuritiesProcessor, InvestmentDebtSecuritiesRecord

class InvestmentDebtSecuritiesStreamingPipeline:
    def __init__(self, batch_size=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.processor = InvestmentDebtSecuritiesProcessor()
        self.batch_size = batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Investment Debt Securities STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_investment_debt_securities_query(self, offset=0):
        """Get the investment debt securities query with ROW_NUMBER() for DB2 batching"""
        
        query = f"""
        SELECT * FROM (
            SELECT
                CURRENT_TIMESTAMP AS reportingDate,
                pa.ACCOUNT_NUMBER AS securityNumber,
                'Treasury bonds' AS securityType,
                'Government of Tanzania' AS securityIssuerName,
                'false' AS ratingStatus,
                'AAA' AS externalIssuerRatting,
                'Grade A' AS gradesUnratedBanks,
                'TANZANIA, UNITED REPUBLIC OF' AS securityIssuerCountry,
                'Other Depository Corporations' AS sectorSnaClassification,
                COALESCE(gte.CURRENCY_SHORT_DES, 'TZS') AS currency,
                
                -- Cost Values
                DECIMAL(gte.DC_AMOUNT, 15, 2) AS orgCostValueAmount,
                DECIMAL(
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 2950.00
                        ELSE gte.DC_AMOUNT
                    END, 15, 2
                ) AS tzsCostValueAmount,
                DECIMAL(
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                        WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT / 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 1.08
                        ELSE NULL
                    END, 15, 2
                ) AS usdCostValueAmount,
                
                -- Face Values
                DECIMAL(gte.DC_AMOUNT, 15, 2) AS orgFaceValueAmount,
                DECIMAL(
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 2950.00
                        ELSE gte.DC_AMOUNT
                    END, 15, 2
                ) AS tzsgFaceValueAmount,
                DECIMAL(
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                        WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT / 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 1.08
                        ELSE NULL
                    END, 15, 2
                ) AS usdgFaceValueAmount,
                
                -- Fair Values
                DECIMAL(gte.DC_AMOUNT, 15, 2) AS orgFairValueAmount,
                DECIMAL(
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 2950.00
                        ELSE gte.DC_AMOUNT
                    END, 15, 2
                ) AS tzsgFairValueAmount,
                DECIMAL(
                    CASE
                        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                        WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT / 2730.50
                        WHEN gte.CURRENCY_SHORT_DES = 'EUR' THEN gte.DC_AMOUNT * 1.08
                        ELSE NULL
                    END, 15, 2
                ) AS usdgFairValueAmount,
                
                -- Other Fields
                DECIMAL(0, 9, 6) AS interestRate,
                gte.TRN_DATE AS purchaseDate,
                gte.AVAILABILITY_DATE AS valueDate,
                gte.AVAILABILITY_DATE AS maturityDate,
                'Hold to Maturity' AS tradingIntent,
                'Unencumbered' AS securityEncumbaranceStatus,
                CASE
                    WHEN gte.AVAILABILITY_DATE < CURRENT_DATE
                    THEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE)
                    ELSE 0
                END AS pastDueDays,
                DECIMAL(0, 15, 2) AS allowanceProbableLoss,
                DECIMAL(0, 15, 2) AS botProvision,
                'Current' AS assetClassificationCategory,
                
                ROW_NUMBER() OVER (ORDER BY gte.TRN_DATE ASC, pa.ACCOUNT_NUMBER ASC) AS rn
                
            FROM GLI_TRX_EXTRACT gte
            LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            LEFT JOIN CUSTOMER c ON gte.CUST_ID = c.CUST_ID
            LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
            WHERE gte.FK_GLG_ACCOUNTACCO IN (
                SELECT ACCOUNT_ID FROM GLG_ACCOUNT WHERE EXTERNAL_GLACCOUNT LIKE '130%'
            )
            AND gte.DC_AMOUNT > 0
            AND gte.TRN_DATE IS NOT NULL
        ) numbered_results
        WHERE rn > {offset} AND rn <= {offset + self.batch_size}
        ORDER BY rn
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT gte
        LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        LEFT JOIN CUSTOMER c ON gte.CUST_ID = c.CUST_ID
        LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
        WHERE gte.FK_GLG_ACCOUNTACCO IN (
            SELECT ACCOUNT_ID FROM GLG_ACCOUNT WHERE EXTERNAL_GLACCOUNT LIKE '130%'
        )
        AND gte.DC_AMOUNT > 0
        AND gte.TRN_DATE IS NOT NULL
        """
    
    
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
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for investment debt securities"""
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
            
            # Declare queue with durability
            channel.queue_declare(queue='investment_debt_securities_queue', durable=True)
            
            connection.close()
            self.logger.info("RabbitMQ queue 'investment_debt_securities_queue' setup complete")
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total investment debt securities records available: {total_available:,}")
            
            # Setup RabbitMQ connection for producer
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
            
            # Process batches
            batch_number = 1
            offset = 0
            
            while offset < total_available:
                # Fetch batch
                with self.db2_conn.get_connection() as conn:
                    cursor = conn.cursor()
                    batch_query = self.get_investment_debt_securities_query(offset)
                    cursor.execute(batch_query)
                    rows = cursor.fetchall()
                
                if not rows:
                    break
                
                self.logger.info(f"Producer: Batch {batch_number} - {len(rows)} records (offset: {offset})")
                
                # Process and publish immediately
                for row in rows:
                    row_without_rn = row[:-1]  # Remove row number
                    record = self.processor.process_record(row_without_rn, 'investmentDebtSecurities')
                    
                    if self.processor.validate_record(record):
                        message = json.dumps(asdict(record), default=str)
                        channel.basic_publish(
                            exchange='',
                            routing_key='investment_debt_securities_queue',
                            body=message,
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        self.total_produced += 1
                
                self.logger.info(f"Producer: Published batch {batch_number} ({self.total_produced} total)")
                
                # Move to next batch
                batch_number += 1
                offset += self.batch_size
                
                # Small delay to prevent overwhelming
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced} records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue"""
        try:
            self.logger.info("Consumer thread started")
            
            # Setup RabbitMQ connection for consumer
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
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = InvestmentDebtSecuritiesRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    self.total_consumed += 1
                    
                    if self.total_consumed % self.batch_size == 0:
                        self.logger.info(f"Consumer: Processed {self.total_consumed} records")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=5)  # Process 5 messages at a time
            channel.basic_consume(queue='investment_debt_securities_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='investment_debt_securities_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    break
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Investment Debt Securities STREAMING pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()
            
            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("Producer thread completed")
            
            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=30)  # Wait up to 30 seconds
            
            if consumer_thread.is_alive():
                self.logger.info("Stopping consumer thread...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=10)
            
            # Final statistics
            self.logger.info(f"""
            Investment Debt Securities Pipeline Summary:
            Records produced: {self.total_produced}
            Records consumed: {self.total_consumed}
            """)
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Investment Debt Securities Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = InvestmentDebtSecuritiesStreamingPipeline(batch_size=args.batch_size)
    
    try:
        if args.mode == 'producer':
            pipeline.producer_thread()
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:  # streaming
            pipeline.run_streaming_pipeline()
            
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()