#!/usr/bin/env python3
"""
Mobile Banking Streaming Pipeline - Producer and Consumer run simultaneously
Uses mobile_banking.sql query with camelCase naming
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
from processors.mobile_banking_processor import MobileBankingProcessor, MobileBankingRecord

class MobileBankingStreamingPipeline:
    def __init__(self, batch_size=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.mobile_processor = MobileBankingProcessor()
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
        
        self.logger.info("🏦 Mobile Banking STREAMING Pipeline initialized")
        self.logger.info(f"📊 Batch size: {batch_size} records per batch")
        self.logger.info("🔄 Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_mobile_banking_query(self, last_trn_date=None, last_trn_snum=None):
        """Get the mobile banking query using cursor-based pagination for DB2"""
        
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
            CURRENT_TIMESTAMP             AS reportingDate,
            gte.TRN_DATE                  AS transactionDate,
            pa.ACCOUNT_NUMBER             AS accountNumber,
            gte.CUST_ID                   AS customerIdentificationNumber,

            -- Deposit, Withdraw or Payment
            CASE
                WHEN gl.EXTERNAL_GLACCOUNT IN ('230000087', '230000123', '144000074') THEN 'Withdraw'
                WHEN gl.EXTERNAL_GLACCOUNT = '144000063' THEN 'Deposit'
                WHEN gl.EXTERNAL_GLACCOUNT IN ('504040001', '504040002', '144000046', '230000064') THEN 'Payment'
                END                       AS mobileTransactionType,
            'Mobile Banking Transactions' AS serviceCategory,
            NULL                          AS subServiceCategory,

            --domestic or international
            'domestic'                    AS serviceStatus,

            VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
            TRIM(gte.FK_USRCODE) || '-' ||
            VARCHAR(gte.LINE_NUM) || '-' ||
            VARCHAR(gte.TRN_DATE) || '-' ||
            VARCHAR(gte.TRN_SNUM)         AS transactionRef,

            'MWCOTZTZ'                    AS benBankOrWalletCode,
            NULL                          AS benAccountOrMobileNumber,

            gte.CURRENCY_SHORT_DES        AS currency,

            -- Original amount (as transacted)
            DECIMAL(gte.DC_AMOUNT, 18, 2) AS orgAmount,

            -- Amount converted to TZS
            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'TZS'
                    THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN DECIMAL(gte.DC_AMOUNT * 2600, 18, 2)
                ELSE NULL
                END                       AS tzsAmount,

            NULL                          AS valueAddedTaxAmount,
            NULL                          AS exciseDutyAmount,
            NULL                          AS electronicLevyAmount,
            
            -- Add TRN_SNUM for cursor tracking
            gte.TRN_SNUM                  AS trn_snum

        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN (SELECT *
                            FROM (SELECT wdc.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                                  FROM W_DIM_CUSTOMER wdc)
                            WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
                 LEFT JOIN (SELECT *
                            FROM (SELECT pa.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                                  FROM PROFITS_ACCOUNT pa
                                  WHERE PRFT_SYSTEM = 3)
                            WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
                 LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
        WHERE gl.EXTERNAL_GLACCOUNT IN (
                                        '230000087',
                                        '144000063',
                                        '230000064',
                                        '504040001',
                                        '504040002',
                                        '144000046',
                                        '144000074',
                                        '230000123'
            )
            {where_clause}
        ORDER BY gte.TRN_DATE ASC, gte.TRN_SNUM ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available mobile banking records"""
        
        query = """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN (SELECT *
                            FROM (SELECT wdc.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                                  FROM W_DIM_CUSTOMER wdc)
                            WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
                 LEFT JOIN (SELECT *
                            FROM (SELECT pa.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                                  FROM PROFITS_ACCOUNT pa
                                  WHERE PRFT_SYSTEM = 3)
                            WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
                 LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
        WHERE gl.EXTERNAL_GLACCOUNT IN (
                                        '230000087',
                                        '144000063',
                                        '230000064',
                                        '504040001',
                                        '504040002',
                                        '144000046',
                                        '144000074',
                                        '230000123'
            )
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
        """Setup RabbitMQ queue for mobile banking"""
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
            
            # Declare mobile banking queue first
            channel.queue_declare(queue='mobile_banking_queue', durable=True)
            
            # Then try to purge existing queue
            try:
                channel.queue_purge('mobile_banking_queue')
                self.logger.info("🧹 Purged existing queue")
            except:
                pass
            
            connection.close()
            self.logger.info("✅ RabbitMQ mobile banking queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - fetches mobile banking data and publishes to queue"""
        try:
            self.logger.info("🏭 Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                count_query = self.get_total_count_query()
                cursor.execute(count_query)
                total_available = cursor.fetchone()[0]
                
            self.logger.info(f"📊 Total available mobile banking records: {total_available:,}")
            
            if total_available == 0:
                self.logger.info("ℹ️ No mobile banking records available")
                self.producer_finished.set()
                return
            
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
            
            # Process batches - cursor-based approach for large dataset
            batch_number = 1
            processed_count = 0
            last_trn_date = None
            last_trn_snum = None
            
            while processed_count < total_available:
                # Fetch batch
                with self.db2_conn.get_connection() as conn:
                    cursor = conn.cursor()
                    batch_query = self.get_mobile_banking_query(last_trn_date, last_trn_snum)
                    cursor.execute(batch_query)
                    rows = cursor.fetchall()
                
                if not rows:
                    break
                
                self.logger.info(f"🏭 Producer: Batch {batch_number} - {len(rows)} mobile banking records")
                
                # Show sample data for first batch
                if batch_number == 1:
                    self.logger.info("📋 Sample mobile banking data from first batch:")
                    for i, row in enumerate(rows[:3], 1):
                        transaction_type = row[4] if row[4] is not None else "N/A"
                        currency = row[11] if row[11] is not None else "N/A"
                        amount = row[12] if row[12] is not None else "N/A"
                        self.logger.info(f"  {i}. Type: {transaction_type}, Currency: {currency}, Amount: {amount}")
                
                # Process and publish immediately
                for row in rows:
                    record = self.mobile_processor.process_record(row, 'mobileBanking')
                    
                    if self.mobile_processor.validate_record(record):
                        message = json.dumps(asdict(record), default=str)
                        channel.basic_publish(
                            exchange='',
                            routing_key='mobile_banking_queue',
                            body=message,
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        self.total_produced += 1
                
                # Update cursor for next batch (use last record's TRN_DATE and TRN_SNUM)
                if rows:
                    last_row = rows[-1]
                    last_trn_date = last_row[1]  # transactionDate
                    last_trn_snum = last_row[17]  # trn_snum (new column we added)
                    
                    self.logger.info(f"🔄 Cursor updated: TRN_DATE={last_trn_date}, TRN_SNUM={last_trn_snum}")
                
                self.logger.info(f"🏭 Producer: Published batch {batch_number} ({self.total_produced} total)")
                
                processed_count += len(rows)
                batch_number += 1
                
                # Break if we got less than batch_size (end of data)
                if len(rows) < self.batch_size:
                    break
                
                # Small delay to prevent overwhelming
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"🏭 Producer finished: {self.total_produced} mobile banking records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Producer error: {e}")
            import traceback
            traceback.print_exc()
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes mobile banking messages from queue"""
        try:
            self.logger.info("🏦 Consumer thread started")
            
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
                    record = MobileBankingRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.mobile_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    self.total_consumed += 1
                    
                    if self.total_consumed % self.batch_size == 0:
                        self.logger.info(f"🏦 Consumer: Processed {self.total_consumed} mobile banking records")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Consumer error processing mobile banking message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=5)  # Process 5 messages at a time
            channel.basic_consume(queue='mobile_banking_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='mobile_banking_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("🏦 Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"❌ Consumer processing error: {e}")
                    break
            
            connection.close()
            self.logger.info(f"🏦 Consumer finished: {self.total_consumed} mobile banking records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            import traceback
            traceback.print_exc()
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("🚀 Starting STREAMING mobile banking pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="MobileBanking-Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="MobileBanking-Producer")
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
            
            self.logger.info(f"📊 STREAMING Mobile Banking Pipeline Results:")
            self.logger.info(f"   Produced: {self.total_produced:,} records")
            self.logger.info(f"   Consumed: {self.total_consumed:,} records")
            
            return self.total_consumed
            
        except Exception as e:
            self.logger.error(f"❌ Streaming mobile banking pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("🏦 MOBILE BANKING STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Minimal queue accumulation")
    print("  - Batch size: 10 records per batch")
    print("  - camelCase table: mobileBanking")
    print("  - camelCase field names")
    print("  - Uses mobile_banking.sql query")
    print("=" * 60)
    
    pipeline = MobileBankingStreamingPipeline(10)
    
    try:
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ STREAMING MOBILE BANKING PIPELINE COMPLETED!")
        print(f"📊 Total mobile banking records processed: {count:,}")
        print("🔍 Key advantages:")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Memory efficient")
        print("  - Fast processing")
        print("  - camelCase naming throughout")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Streaming mobile banking pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()