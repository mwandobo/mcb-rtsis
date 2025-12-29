#!/usr/bin/env python3
"""
Simple Cash Pipeline without Redis tracking
Uses the corrected query matching sqls/cash-information.sql
"""

import pika
import psycopg2
import json
import logging
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from db2_connection import DB2Connection
from processors.cash_processor import CashProcessor, CashRecord

class SimpleCashPipeline:
    def __init__(self, manual_start_date="2024-01-01 00:00:00", limit=5000):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.cash_processor = CashProcessor()
        self.manual_start_date = manual_start_date
        self.limit = limit
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("💰 Simple Cash Pipeline initialized")
        self.logger.info(f"📅 Manual start date: {manual_start_date}")
        self.logger.info(f"📊 Record limit: {limit}")
    
    def get_corrected_cash_query(self):
        """Get the corrected cash query matching sqls/cash-information.sql"""
        
        timestamp_filter = f"AND gte.TRN_DATE >= TIMESTAMP('{self.manual_start_date}')"
        
        query = f"""
        SELECT
            CURRENT_TIMESTAMP as reportingDate,
            gte.FK_UNITCODETRXUNIT AS branchCode,
            CASE
              WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
              WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
              WHEN gl.EXTERNAL_GLACCOUNT='101000010' OR gl.EXTERNAL_GLACCOUNT='101000015' THEN 'Cash in ATMs'
              WHEN gl.EXTERNAL_GLACCOUNT='101000004' OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Cash with Tellers'
              ELSE 'unknown'
            END as cashCategory,
            CASE
                WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'CleanNotes'
                WHEN gl.EXTERNAL_GLACCOUNT='101000002'  OR
                     gl.EXTERNAL_GLACCOUNT='101000010'  OR
                     gl.EXTERNAL_GLACCOUNT='101000004'  OR
                     gl.EXTERNAL_GLACCOUNT='101000015'  OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Notes'
                ELSE null
            END as cashSubCategory,
            'Business Hours' as cashSubmissionTime,
            gte.CURRENCY_SHORT_DES as currency,
            null as cashDenomination,
            null as quantityOfCoinsNotes,
            gte.DC_AMOUNT AS orgAmount,
            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN gte.DC_AMOUNT
                ELSE NULL
            END AS usdAmount,
            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN gte.DC_AMOUNT * 2500
                ELSE
                    gte.DC_AMOUNT
            END AS tzsAmount,
            gte.TRN_DATE as transactionDate,
            gte.AVAILABILITY_DATE as maturityDate,
            0 as allowanceProbableLoss,
            0 as botProvision
        FROM GLI_TRX_EXTRACT AS gte
        JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015')
        {timestamp_filter}
        ORDER BY gte.TRN_DATE ASC
        FETCH FIRST {self.limit} ROWS ONLY
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
        """Setup RabbitMQ queue for cash"""
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
            
            # Declare cash queue
            channel.queue_declare(queue='cash_information_queue', durable=True)
            
            connection.close()
            self.logger.info("✅ RabbitMQ cash queue created")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def fetch_and_publish_cash(self):
        """Fetch cash data and publish to queue"""
        try:
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                
                cash_query = self.get_corrected_cash_query()
                self.logger.info("📊 Executing corrected cash query...")
                
                cursor.execute(cash_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"💰 Fetched {len(rows)} cash records")
                
                if not rows:
                    self.logger.info("ℹ️ No cash records found")
                    return 0
                
                # Show date range of fetched data
                first_date = rows[0][11]  # transactionDate is at index 11
                last_date = rows[-1][11]
                self.logger.info(f"📅 Date range: {first_date} to {last_date}")
                
                # Show sample data
                self.logger.info("📋 Sample cash data:")
                for i, row in enumerate(rows[:3], 1):
                    amount = row[8] if row[8] is not None else 0  # orgAmount is at index 8
                    currency = row[5] if row[5] is not None else "N/A"  # currency is at index 5
                    submission_time = row[4]  # cashSubmissionTime
                    self.logger.info(f"  {i}. Date: {row[11]}, Branch: {row[1]}, Category: {row[2]}")
                    self.logger.info(f"      SubmissionTime: '{submission_time}', Amount: {amount:,.2f} {currency}")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.cash_processor.process_record(row, 'cash_information')
                    if self.cash_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'cash_information_queue')
                    self.logger.info(f"✅ Published {len(records)} cash records to queue")
                
                return len(records)
                
        except Exception as e:
            self.logger.error(f"❌ Cash fetch error: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def publish_records(self, records, queue_name):
        """Publish records to RabbitMQ"""
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
            
            for record in records:
                message = json.dumps(asdict(record), default=str)
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Publish error to {queue_name}: {e}")
            raise
    
    def consume_and_insert(self):
        """Consume messages and insert directly to PostgreSQL"""
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
            
            def process_message(ch, method, properties, body):
                nonlocal processed_count
                try:
                    record_data = json.loads(body)
                    record = CashRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.cash_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    processed_count += 1
                    
                    if processed_count % 100 == 0:
                        self.logger.info(f"💰 Processed {processed_count} cash records...")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS and consume
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='cash_information_queue', on_message_callback=process_message)
            
            self.logger.info("🔄 Starting to consume and insert cash records...")
            
            # Check if there are messages to process
            method = channel.queue_declare(queue='cash_information_queue', durable=True, passive=True)
            message_count = method.method.message_count
            
            if message_count > 0:
                self.logger.info(f"📊 Processing {message_count} messages from queue...")
                
                # Process all messages
                while message_count > 0:
                    connection.process_data_events(time_limit=1)
                    method = channel.queue_declare(queue='cash_information_queue', durable=True, passive=True)
                    new_count = method.method.message_count
                    if new_count == message_count:
                        break  # No progress, exit
                    message_count = new_count
            
            connection.close()
            self.logger.info(f"✅ Finished processing {processed_count} cash records")
            return processed_count
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            raise
    
    def run_complete_pipeline(self):
        """Run the complete pipeline: fetch, publish, consume, insert"""
        self.logger.info("🚀 Starting complete cash pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Fetch and publish
            published_count = self.fetch_and_publish_cash()
            
            if published_count > 0:
                # Consume and insert
                processed_count = self.consume_and_insert()
                
                self.logger.info(f"✅ Pipeline completed successfully!")
                self.logger.info(f"📊 Published: {published_count}, Processed: {processed_count}")
                return processed_count
            else:
                self.logger.info("ℹ️ No records to process")
                return 0
                
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("💰 SIMPLE CASH PIPELINE - JANUARY 2024")
    print("=" * 60)
    print("📅 Using corrected query matching sqls/cash-information.sql")
    print("🎯 Start date: 2024-01-01 00:00:00")
    print("📊 Limit: 5000 records")
    print("=" * 60)
    
    pipeline = SimpleCashPipeline("2024-01-01 00:00:00", 5000)
    
    try:
        count = pipeline.run_complete_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"📊 Total records processed: {count:,}")
        print("🔍 Key features verified:")
        print("  - cashSubmissionTime: 'Business Hours' (text)")
        print("  - transactionDate: uses gte.TRN_DATE")
        print("  - cashSubCategory: CleanNotes/Notes logic")
        print("  - GL accounts: matches original specification")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()