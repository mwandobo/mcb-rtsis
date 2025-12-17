#!/usr/bin/env python3
"""
Cash Pipeline with Proper Timestamp Tracking
"""

import pika
import psycopg2
from db2_connection import DB2Connection
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import asdict

from config import Config
from processors.cash_processor import CashProcessor, CashRecord
from pipeline_tracker import PipelineTracker

class TrackedCashPipeline:
    def __init__(self, manual_start_date=None, lookback_days=7, limit=1000):
        """
        Initialize cash pipeline with tracking
        
        Args:
            manual_start_date (str): Manual start date in 'YYYY-MM-DD HH:MM:SS' format
            lookback_days (int): Days to look back for first run (default: 7)
            limit (int): Number of records to fetch per run (default: 1000)
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.tracker = PipelineTracker()
        self.running = True
        self.manual_start_date = manual_start_date
        self.lookback_days = lookback_days
        self.limit = limit
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.cash_processor = CashProcessor()
        
        self.logger.info("💰 Tracked Cash Pipeline initialized")
        if manual_start_date:
            self.logger.info(f"📅 Manual start date: {manual_start_date}")
        else:
            self.logger.info(f"📅 Lookback days: {lookback_days}")
        self.logger.info(f"📊 Record limit: {limit}")
        
    def get_cash_query_with_tracking(self):
        """Get cash query with proper timestamp tracking"""
        
        # Check if we have a manual start date or need to use tracking
        if self.manual_start_date:
            # Use manual start date (for testing)
            timestamp_filter = f"AND gte.TRN_DATE >= TIMESTAMP('{self.manual_start_date}')"
            self.logger.info(f"🔧 Using manual start date: {self.manual_start_date}")
        else:
            # Use tracking system
            timestamp_filter = self.tracker.get_incremental_query_filter(
                'cash_information', 
                'gte.TRN_DATE', 
                self.lookback_days
            )
            self.logger.info(f"📅 Using tracking filter: {timestamp_filter}")
        
        query = f"""
        SELECT 
            gte.TRN_DATE,
            CURRENT_TIMESTAMP AS REPORTINGDATE,
            gte.FK_UNITCODETRXUNIT AS BRANCHCODE,
            CASE 
                WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                WHEN gl.EXTERNAL_GLACCOUNT IN ('101000010','101000015') THEN 'Cash in ATMs'
                WHEN gl.EXTERNAL_GLACCOUNT IN ('101000004','101000011') THEN 'Cash in Teller'
                ELSE 'Other cash'
            END AS CASHCATEGORY,
            gte.CURRENCY_SHORT_DES AS CURRENCY,
            gte.DC_AMOUNT AS ORGAMOUNT,
            CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT ELSE NULL END AS USDAMOUNT,
            CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT*2500 ELSE gte.DC_AMOUNT END AS TZSAMOUNT,
            gte.TRN_DATE AS TRANSACTIONDATE,
            gte.AVAILABILITY_DATE AS MATURITYDATE,
            CAST(0 AS DECIMAL(18,2)) AS ALLOWANCEPROBABLELOSS,
            CAST(0 AS DECIMAL(18,2)) AS BOTPROVISSION
        FROM GLI_TRX_EXTRACT gte 
        JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
        WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
        {timestamp_filter}
        ORDER BY gte.TRN_DATE ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
        
        return query
        
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
        """Fetch cash data with tracking and publish to queue"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                cash_query = self.get_cash_query_with_tracking()
                self.logger.info("📊 Executing tracked cash query...")
                
                cursor.execute(cash_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"💰 Fetched {len(rows)} cash records")
                
                if not rows:
                    self.logger.info("ℹ️ No new cash records found")
                    return 0, None
                
                # Show date range of fetched data
                first_date = rows[0][0]  # TRN_DATE is first column
                last_date = rows[-1][0]
                self.logger.info(f"📅 Date range: {first_date} to {last_date}")
                
                # Show sample data
                self.logger.info("📋 Sample cash data:")
                for i, row in enumerate(rows[:3], 1):
                    self.logger.info(f"  {i}. Date: {row[0]}, Branch: {row[2]}, Category: {row[3]}, Amount: {row[6]:,.2f} {row[5]}")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.cash_processor.process_record(row, 'cash_information')
                    if self.cash_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'cash_information_queue')
                    self.logger.info(f"✅ Published {len(records)} cash records to queue")
                
                return len(records), str(last_date)
                
        except Exception as e:
            self.logger.error(f"❌ Cash fetch error: {e}")
            self.tracker.update_processing_stats('cash_information', 0, has_error=True)
            return 0, None
    
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
    
    def consume_cash_queue(self):
        """Consume cash records from queue"""
        self.logger.info("🔄 Starting cash consumer...")
        
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
            
            def process_cash_message(ch, method, properties, body):
                nonlocal processed_count
                try:
                    record_data = json.loads(body)
                    record = CashRecord(**record_data)
                    
                    self.logger.info(f"💰 Processing: Date {record.transaction_date}, Branch {record.branch_code}, {record.cash_category}, {record.amount_local:,.2f} {record.currency}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.cash_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    