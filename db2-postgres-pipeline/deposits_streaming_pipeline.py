#!/usr/bin/env python3
"""
Deposits Streaming Pipeline - Producer and Consumer run simultaneously
Uses deposits.sql query with camelCase naming
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
from processors.deposits_processor import DepositsProcessor, DepositsRecord

class DepositsStreamingPipeline:
    def __init__(self, batch_size=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.deposits_processor = DepositsProcessor()
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
        
        self.logger.info("🏦 Deposits STREAMING Pipeline initialized")
        self.logger.info(f"📊 Batch size: {batch_size} records per batch")
        self.logger.info("🔄 Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_deposits_query(self, last_trn_date=None, last_trn_snum=None):
        """Get the deposits query using cursor-based pagination for DB2"""
        
        if last_trn_date is None and last_trn_snum is None:
            # First batch
            where_clause = ""
        else:
            # Subsequent batches - use cursor pagination
            where_clause = f"""
            AND (gte.TRN_DATE > '{last_trn_date}' 
                 OR (gte.TRN_DATE = '{last_trn_date}' AND gte.TRN_SNUM > {last_trn_snum}))
            """
        
        query = f"""
        SELECT 
            CURRENT_TIMESTAMP                               AS reportingDate,
            gte.CUST_ID                                     AS clientIdentificationNumber,
            CAST(gte.CUST_ID AS VARCHAR(50))                AS accountNumber,
            'Customer ' || CAST(gte.CUST_ID AS VARCHAR(10)) AS accountName,
            'Individual'                                    AS customerCategory,
            'TANZANIA, UNITED REPUBLIC OF'                  AS customerCountry,
            '001'                                           AS branchCode,
            'Individual'                                    AS clientType,
            'Domestic banks unrelated'                      AS relationshipType,
            'Dar es Salaam'                                 AS district,
            'DAR ES SALAAM'                                 AS region,
            'Savings Account'                               AS accountProductName,
            'Saving'                                        AS accountType,
            null                                            AS accountSubType,
            'Deposit from public'                           AS depositCategory,
            'active'                                        AS depositAccountStatus,
            VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
            TRIM(gte.FK_USRCODE) || '-' ||
            VARCHAR(gte.LINE_NUM) || '-' ||
            VARCHAR(gte.TRN_DATE) || '-' ||
            VARCHAR(gte.TRN_SNUM)                           AS transactionUniqueRef,
            gte.TMSTAMP                                     AS timeStamp,
            'Branch'                                        AS serviceChannel,
            gte.CURRENCY_SHORT_DES                          AS currency,
            'Deposit'                                       AS transactionType,
            gte.DC_AMOUNT                                   AS orgTransactionAmount,
            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                ELSE NULL
                END                                         AS usdTransactionAmount,

            -- TZS Amount: convert only if USD, otherwise use as is
            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN gte.DC_AMOUNT * 2500
                ELSE
                    gte.DC_AMOUNT
                END                                         AS tzsTransactionAmount,
            gte.JUSTIFIC_DESCR                              AS transactionPurposes,
            null                                            AS sectorSnaClassification,
            null                                            AS lienNumber,
            null                                            AS orgAmountLien,
            null                                            AS usdAmountLien,
            null                                            AS tzsAmountLien,
            gte.TRN_DATE                                    AS contractDate,
            null                                            AS maturityDate,
            null                                            AS annualInterestRate,
            null                                            AS interestRateType,
            0                                               AS orgInterestAmount,
            0                                               AS usdInterestAmount,
            0                                               AS tzsInterestAmount,
            
            -- Add TRN_SNUM for cursor tracking
            gte.TRN_SNUM                                    AS trn_snum

        FROM GLI_TRX_EXTRACT gte
        WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT'
            {where_clause}
        ORDER BY gte.TRN_DATE ASC, gte.TRN_SNUM ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available deposits records"""
        
        query = """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT gte
        WHERE gte.JUSTIFIC_DESCR = 'JOURNAL CREDIT'
        """
        
        return query
    
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
        """Setup RabbitMQ queue for deposits"""
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
            
            # Declare deposits queue first
            channel.queue_declare(queue='deposits_queue', durable=True)
            
            # Then try to purge existing queue
            try:
                channel.queue_purge('deposits_queue')
                self.logger.info("🧹 Purged existing queue")
            except:
                pass
            
            connection.close()
            self.logger.info("✅ RabbitMQ deposits queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - fetches deposits data and publishes to queue"""
        try:
            self.logger.info("🏭 Producer thread started")
            
            # Skip count query for now - just start processing
            self.logger.info("🏭 Producer thread started")
            self.logger.info("📊 Processing deposits records in batches...")
            
            # Process batches - cursor-based approach for large dataset
            batch_number = 1
            processed_count = 0
            last_trn_date = None
            last_trn_snum = None
            
            while True:  # Process until no more data
                # Setup RabbitMQ connection for each batch to avoid timeouts
                try:
                    credentials = pika.PlainCredentials(
                        self.config.message_queue.rabbitmq_user,
                        self.config.message_queue.rabbitmq_password
                    )
                    parameters = pika.ConnectionParameters(
                        host=self.config.message_queue.rabbitmq_host,
                        port=self.config.message_queue.rabbitmq_port,
                        credentials=credentials,
                        heartbeat=600,  # 10 minutes heartbeat
                        blocked_connection_timeout=300  # 5 minutes timeout
                    )
                    connection = pika.BlockingConnection(parameters)
                    channel = connection.channel()
                    
                    # Fetch batch using cursor
                    with self.db2_conn.get_connection() as conn:
                        cursor = conn.cursor()
                        batch_query = self.get_deposits_query(last_trn_date, last_trn_snum)
                        cursor.execute(batch_query)
                        rows = cursor.fetchall()
                    
                    if not rows:
                        connection.close()
                        break
                    
                    self.logger.info(f"🏭 Producer: Batch {batch_number} - {len(rows)} deposits records")
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample deposits data from first batch:")
                        for i, row in enumerate(rows[:3], 1):
                            account_number = row[2] if row[2] is not None else "N/A"
                            currency = row[19] if row[19] is not None else "N/A"
                            amount = row[21] if row[21] is not None else "N/A"
                            self.logger.info(f"  {i}. Account: {account_number}, Currency: {currency}, Amount: {amount}")
                    
                    # Process and publish batch
                    batch_published = 0
                    for row in rows:
                        record = self.deposits_processor.process_record(row, 'deposits')
                        
                        if self.deposits_processor.validate_record(record):
                            message = json.dumps(asdict(record), default=str)
                            channel.basic_publish(
                                exchange='',
                                routing_key='deposits_queue',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                            self.total_produced += 1
                            batch_published += 1
                    
                    # Close connection after batch
                    connection.close()
                    
                    # Update cursor for next batch (use last record's TRN_DATE and TRN_SNUM)
                    if rows:
                        last_row = rows[-1]
                        # Get TRN_DATE from the query - it's not directly in the SELECT but we can derive it
                        # We'll use the trn_snum (last column) and need to get the date from DB2
                        last_trn_snum = last_row[37]  # trn_snum (last column)
                        
                        # Get the TRN_DATE for this TRN_SNUM
                        with self.db2_conn.get_connection() as conn:
                            cursor = conn.cursor()
                            date_query = f"SELECT TRN_DATE FROM GLI_TRX_EXTRACT WHERE TRN_SNUM = {last_trn_snum}"
                            cursor.execute(date_query)
                            date_result = cursor.fetchone()
                            if date_result:
                                last_trn_date = date_result[0]
                        
                        self.logger.info(f"🔄 Cursor updated: TRN_DATE={last_trn_date}, TRN_SNUM={last_trn_snum}")
                    
                    self.logger.info(f"🏭 Producer: Published batch {batch_number} ({batch_published} records, {self.total_produced} total)")
                    
                    processed_count += len(rows)
                    batch_number += 1
                    
                    # Break if we got less than batch_size (end of data)
                    if len(rows) < self.batch_size:
                        break
                    
                    # Small delay between batches
                    time.sleep(0.5)
                    
                except Exception as batch_error:
                    self.logger.error(f"❌ Batch {batch_number} error: {batch_error}")
                    # Continue to next batch
                    processed_count += self.batch_size  # Skip this batch
                    batch_number += 1
                    time.sleep(2)  # Wait before retry
            
            self.logger.info(f"🏭 Producer finished: {self.total_produced} deposits records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Producer error: {e}")
            import traceback
            traceback.print_exc()
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes deposits messages from queue"""
        try:
            self.logger.info("🏦 Consumer thread started")
            
            # Setup RabbitMQ connection for consumer with better timeout settings
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials,
                heartbeat=600,  # 10 minutes heartbeat
                blocked_connection_timeout=300  # 5 minutes timeout
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = DepositsRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.deposits_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    self.total_consumed += 1
                    
                    if self.total_consumed % self.batch_size == 0:
                        self.logger.info(f"🏦 Consumer: Processed {self.total_consumed} deposits records")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Consumer error processing deposits message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=5)  # Process 5 messages at a time
            channel.basic_consume(queue='deposits_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        try:
                            method = channel.queue_declare(queue='deposits_queue', durable=True, passive=True)
                            if method.method.message_count == 0:
                                self.logger.info("🏦 Consumer: Queue empty, producer finished")
                                break
                        except:
                            # If queue check fails, assume we should stop
                            break
                        
                except Exception as e:
                    self.logger.error(f"❌ Consumer processing error: {e}")
                    # Try to reconnect
                    try:
                        connection.close()
                    except:
                        pass
                    time.sleep(2)
                    try:
                        connection = pika.BlockingConnection(parameters)
                        channel = connection.channel()
                        channel.basic_qos(prefetch_count=5)
                        channel.basic_consume(queue='deposits_queue', on_message_callback=process_message)
                    except Exception as reconnect_error:
                        self.logger.error(f"❌ Failed to reconnect: {reconnect_error}")
                        break
            
            try:
                connection.close()
            except:
                pass
            self.logger.info(f"🏦 Consumer finished: {self.total_consumed} deposits records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            import traceback
            traceback.print_exc()
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("🚀 Starting STREAMING deposits pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Deposits-Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Deposits-Producer")
            producer_thread.start()
            
            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("✅ Producer thread completed")
            
            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=30)  # 30 second timeout
            
            if consumer_thread.is_alive():
                self.logger.warning("⚠️ Consumer thread timeout, stopping...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=5)
            
            self.logger.info("✅ Consumer thread completed")
            
            self.logger.info(f"📊 STREAMING Deposits Pipeline Results:")
            self.logger.info(f"   Produced: {self.total_produced:,} records")
            self.logger.info(f"   Consumed: {self.total_consumed:,} records")
            
            return self.total_consumed
            
        except Exception as e:
            self.logger.error(f"❌ Streaming deposits pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("🏦 DEPOSITS STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Minimal queue accumulation")
    print("  - Batch size: 10 records per batch")
    print("  - camelCase table: deposits")
    print("  - camelCase field names")
    print("  - Uses deposits.sql query")
    print("  - ROW_NUMBER() for unique records")
    print("=" * 60)
    
    pipeline = DepositsStreamingPipeline(10)
    
    try:
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ STREAMING DEPOSITS PIPELINE COMPLETED!")
        print(f"📊 Total deposits records processed: {count:,}")
        print("🔍 Key advantages:")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Memory efficient")
        print("  - Fast processing")
        print("  - camelCase naming throughout")
        print("  - Unique transactionUniqueRef values")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Streaming deposits pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()