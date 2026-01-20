#!/usr/bin/env python3
"""
POS Information Streaming Pipeline - Producer and Consumer run simultaneously
Uses pos-v1.sql query with camelCase naming
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
from processors.pos_information_processor import PosInformationProcessor, PosInformationRecord

class PosInformationStreamingPipeline:
    def __init__(self, batch_size=10):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.pos_processor = PosInformationProcessor()
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
        
        self.logger.info("🏪 POS Information STREAMING Pipeline initialized")
        self.logger.info(f"📊 Batch size: {batch_size} records per batch")
        self.logger.info("🔄 Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_pos_information_query(self, offset=0):
        """Get the POS information query with ROW_NUMBER() for DB2 batching"""
        
        query = f"""
        SELECT * FROM (
            SELECT 
                VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
                201                                                  AS posBranchCode,
                at.FK_USRCODE                                        AS posNumber,
                'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
                'Bank Agent'                                         AS posHolderCategory,
                'Selcom'                                             AS posHolderName,
                null                                                 AS posHolderNin,
                '103-847-451'                                        AS posHolderTin,
                NULL                                                 AS postalCode,
                COALESCE(region_lkp.BOT_REGION, 'N/A')               AS region,
                COALESCE(district_lkp.BOT_DISTRICT, 'N/A')           AS district,
                COALESCE(ward_lkp.BOT_WARD, 'N/A')                   AS ward,
                'N/A'                                                AS street,
                'N/A'                                                AS houseNumber,
                al.GPS                                               AS gpsCoordinates,
                '230000070'                                          AS linkedAccount,
                VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
                NULL                                                 AS returnDate,
                
                ROW_NUMBER() OVER (ORDER BY at.FK_USRCODE ASC) AS rn
                
            FROM AGENT_TERMINAL at
            JOIN (SELECT DISTINCT RIGHT(TRIM(TERMINAL_ID), 8) AS TERMINAL_ID_8, GPS
                  FROM AGENTS_LIST) al
                 ON al.TERMINAL_ID_8 = TRIM(at.FK_USRCODE)
            LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                              bl.REGION                             AS BOT_REGION,
                              ROW_NUMBER() OVER (
                                  PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                                  ORDER BY
                                      CASE
                                          WHEN UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION)) THEN 1
                                          WHEN UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%' THEN 2
                                          ELSE 99
                                          END,
                                      LENGTH(TRIM(bl.REGION)) DESC
                                  )                                 AS rn
                       FROM AGENTS_LIST al
                       JOIN BANK_LOCATION_LOOKUP_V2 bl
                            ON UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION))
                                OR (
                                   UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%'
                                       AND LENGTH(TRIM(bl.REGION)) >= 4
                                   )) region_lkp
                      ON region_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                          AND region_lkp.rn = 1
            LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                              bl.DISTRICT                           AS BOT_DISTRICT,
                              ROW_NUMBER() OVER (
                                  PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                                  ORDER BY
                                      CASE
                                          WHEN UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT)) THEN 1
                                          WHEN UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                              AND LENGTH(TRIM(bl.DISTRICT)) >= 4 THEN 2
                                          ELSE 99
                                          END,
                                      LENGTH(TRIM(bl.DISTRICT)) DESC
                                  )                                 AS rn
                       FROM AGENTS_LIST al
                       JOIN BANK_LOCATION_LOOKUP_V2 bl
                            ON (
                                UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT))
                                    OR (
                                    UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                        AND LENGTH(TRIM(bl.DISTRICT)) >= 4
                                    )
                                )
                       WHERE TRIM(al.DISTRICT) IS NOT NULL
                         AND TRIM(al.DISTRICT) <> '') district_lkp
                      ON district_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                          AND district_lkp.rn = 1
            LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                              bl.WARD                               AS BOT_WARD,
                              ROW_NUMBER() OVER (
                                  PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                                  ORDER BY
                                      CASE
                                          WHEN UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD)) THEN 1
                                          WHEN UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                              AND LENGTH(TRIM(bl.WARD)) >= 4 THEN 2
                                          ELSE 99
                                          END,
                                      LENGTH(TRIM(bl.WARD)) DESC
                                  )                                 AS rn
                       FROM AGENTS_LIST al
                       JOIN BANK_LOCATION_LOOKUP_V2 bl
                            ON (
                                UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD))
                                    OR (
                                    UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                        AND LENGTH(TRIM(bl.WARD)) >= 4
                                    )
                                )
                       WHERE TRIM(al.LOCATION) IS NOT NULL
                         AND TRIM(al.LOCATION) <> '') ward_lkp
                      ON ward_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                          AND ward_lkp.rn = 1
        ) numbered_results
        WHERE rn > {offset} AND rn <= {offset + self.batch_size}
        ORDER BY rn
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available POS records"""
        
        query = """
        SELECT COUNT(*) as total_count
        FROM AGENT_TERMINAL at
        JOIN (SELECT DISTINCT RIGHT(TRIM(TERMINAL_ID), 8) AS TERMINAL_ID_8, GPS
              FROM AGENTS_LIST) al
             ON al.TERMINAL_ID_8 = TRIM(at.FK_USRCODE)
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
        """Setup RabbitMQ queue for POS information"""
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
            
            # Declare POS information queue first
            channel.queue_declare(queue='pos_information_queue', durable=True)
            
            # Then try to purge existing queue
            try:
                channel.queue_purge('pos_information_queue')
                self.logger.info("🧹 Purged existing queue")
            except:
                pass
            
            connection.close()
            self.logger.info("✅ RabbitMQ POS information queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - fetches POS data and publishes to queue"""
        try:
            self.logger.info("🏭 Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                count_query = self.get_total_count_query()
                cursor.execute(count_query)
                total_available = cursor.fetchone()[0]
                
            self.logger.info(f"📊 Total available POS records: {total_available:,}")
            
            if total_available == 0:
                self.logger.info("ℹ️ No POS records available")
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
            
            # Process batches
            batch_number = 1
            offset = 0
            
            while offset < total_available:
                # Fetch batch
                with self.db2_conn.get_connection() as conn:
                    cursor = conn.cursor()
                    batch_query = self.get_pos_information_query(offset)
                    cursor.execute(batch_query)
                    rows = cursor.fetchall()
                
                if not rows:
                    break
                
                self.logger.info(f"🏭 Producer: Batch {batch_number} - {len(rows)} POS records (offset: {offset})")
                
                # Show sample data for first batch
                if batch_number == 1:
                    self.logger.info("📋 Sample POS data from first batch:")
                    for i, row in enumerate(rows[:3], 1):
                        pos_number = row[2] if row[2] is not None else "N/A"
                        qr_code = row[3] if row[3] is not None else "N/A"
                        region = row[9] if row[9] is not None else "N/A"
                        self.logger.info(f"  {i}. POS: {pos_number}, QR: {qr_code}, Region: {region}")
                
                # Process and publish immediately
                for row in rows:
                    row_without_rn = row[:-1]  # Remove row number
                    record = self.pos_processor.process_record(row_without_rn, 'posInformation')
                    
                    if self.pos_processor.validate_record(record):
                        message = json.dumps(asdict(record), default=str)
                        channel.basic_publish(
                            exchange='',
                            routing_key='pos_information_queue',
                            body=message,
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        self.total_produced += 1
                
                self.logger.info(f"🏭 Producer: Published batch {batch_number} ({self.total_produced} total)")
                
                # Move to next batch
                batch_number += 1
                offset += self.batch_size
                
                # Small delay to prevent overwhelming
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"🏭 Producer finished: {self.total_produced} POS records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Producer error: {e}")
            import traceback
            traceback.print_exc()
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes POS messages from queue"""
        try:
            self.logger.info("🏪 Consumer thread started")
            
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
                    record = PosInformationRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.pos_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    self.total_consumed += 1
                    
                    if self.total_consumed % self.batch_size == 0:
                        self.logger.info(f"🏪 Consumer: Processed {self.total_consumed} POS records")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ Consumer error processing POS message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=5)  # Process 5 messages at a time
            channel.basic_consume(queue='pos_information_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='pos_information_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("🏪 Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"❌ Consumer processing error: {e}")
                    break
            
            connection.close()
            self.logger.info(f"🏪 Consumer finished: {self.total_consumed} POS records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ Consumer error: {e}")
            import traceback
            traceback.print_exc()
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("🚀 Starting STREAMING POS information pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="POS-Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="POS-Producer")
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
            
            self.logger.info(f"📊 STREAMING POS Pipeline Results:")
            self.logger.info(f"   Produced: {self.total_produced:,} records")
            self.logger.info(f"   Consumed: {self.total_consumed:,} records")
            
            return self.total_consumed
            
        except Exception as e:
            self.logger.error(f"❌ Streaming POS pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("🏪 POS INFORMATION STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Minimal queue accumulation")
    print("  - Batch size: 10 records per batch")
    print("  - camelCase table: posInformation")
    print("  - camelCase field names")
    print("  - Uses pos-v1.sql query")
    print("=" * 60)
    
    pipeline = PosInformationStreamingPipeline(10)
    
    try:
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ STREAMING POS PIPELINE COMPLETED!")
        print(f"📊 Total POS records processed: {count:,}")
        print("🔍 Key advantages:")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Memory efficient")
        print("  - Fast processing")
        print("  - camelCase naming throughout")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Streaming POS pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()