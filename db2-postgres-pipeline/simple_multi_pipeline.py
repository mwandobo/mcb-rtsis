#!/usr/bin/env python3
"""
Simple Multi-table Pipeline - Actually Working Version
"""

import pika
import psycopg2
from db2_connection import DB2Connection
import redis
import json
import time
import logging
import threading
from datetime import datetime
from contextlib import contextmanager
from dataclasses import asdict

from config import Config
from processors.cash_processor import CashProcessor, CashRecord
from processors.assets_processor import AssetsProcessor, AssetsRecord
from processors.bot_balances_processor import BotBalancesProcessor, BotBalancesRecord
from processors.mnos_processor import MnosProcessor, MnosRecord
from processors.other_banks_processor import OtherBanksProcessor, OtherBanksRecord
from processors.other_assets_processor import OtherAssetsProcessor, OtherAssetsRecord
from processors.overdraft_processor import OverdraftProcessor, OverdraftRecord
from processors.branch_processor import BranchProcessor, BranchRecord
from processors.agent_processor import AgentProcessor, AgentRecord
from pipeline_tracker import PipelineTracker

class SimpleMultiPipeline:
    def __init__(self):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.tracker = PipelineTracker()
        self.running = True
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processors
        self.cash_processor = CashProcessor()
        self.assets_processor = AssetsProcessor()
        self.bot_balances_processor = BotBalancesProcessor()
        self.mnos_processor = MnosProcessor()
        self.other_banks_processor = OtherBanksProcessor()
        self.other_assets_processor = OtherAssetsProcessor()
        self.overdraft_processor = OverdraftProcessor()
        self.branch_processor = BranchProcessor()
        self.agent_processor = AgentProcessor()
        
        self.logger.info("Multi-table pipeline initialized with tracking")
        
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
    
    def setup_rabbitmq_queues(self):
        """Setup RabbitMQ queues for both tables"""
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
            
            # Declare queues for all tables
            channel.queue_declare(queue='cash_information_queue', durable=True)
            channel.queue_declare(queue='asset_owned_queue', durable=True)
            channel.queue_declare(queue='balances_bot_queue', durable=True)
            channel.queue_declare(queue='balances_with_mnos_queue', durable=True)
            channel.queue_declare(queue='balance_with_other_banks_queue', durable=True)
            channel.queue_declare(queue='other_assets_queue', durable=True)
            channel.queue_declare(queue='overdraft_queue', durable=True)
            channel.queue_declare(queue='branch_queue', durable=True)
            channel.queue_declare(queue='agents_queue', durable=True)
            
            connection.close()
            self.logger.info("✅ RabbitMQ queues created: cash_information_queue, asset_owned_queue, balances_bot_queue, balances_with_mnos_queue, balance_with_other_banks_queue, other_assets_queue, overdraft_queue")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queues: {e}")
            raise
    
    def fetch_and_publish_cash(self):
        """Fetch cash data with tracking and publish to queue"""
        cash_config = self.config.tables['cash_information']
        
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Get incremental query with tracking
                timestamp_filter = self.tracker.get_incremental_query_filter(
                    'cash_information', 
                    'gte.TMSTAMP', 
                    default_lookback_days=7
                )
                
                # Build query with tracking using TMSTAMP
                cash_query = f"""
                SELECT 
                    gte.TMSTAMP,
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
                ORDER BY gte.TMSTAMP ASC
                FETCH FIRST 1000 ROWS ONLY
                """
                
                self.logger.info(f"📊 Executing cash query with tracking filter: {timestamp_filter}")
                cursor.execute(cash_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"💰 Fetched {len(rows)} new cash records")
                
                if not rows:
                    self.logger.info("ℹ️ No new cash records found")
                    return 0
                
                # Show timestamp range (TMSTAMP is now first column)
                first_timestamp = rows[0][0]
                last_timestamp = rows[-1][0]
                self.logger.info(f"📅 Timestamp range: {first_timestamp} to {last_timestamp}")
                
                # Process and publish (adjust for new column order)
                records = []
                for row in rows:
                    # Skip TMSTAMP column for processor (it expects old format)
                    adjusted_row = row[1:]  # Remove TMSTAMP, keep TRN_DATE and rest
                    record = self.cash_processor.process_record(adjusted_row, 'cash_information')
                    if self.cash_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'cash_information_queue')
                    self.logger.info(f"✅ Published {len(records)} cash records")
                    
                    # Update tracking with the latest TMSTAMP
                    self.tracker.set_last_processed_timestamp('cash_information', str(last_timestamp))
                    self.tracker.update_processing_stats('cash_information', len(records))
                
                return len(records)
                
        except Exception as e:
            self.logger.error(f"❌ Cash fetch error: {e}")
            self.tracker.update_processing_stats('cash_information', 0, has_error=True)
            return 0
    
    def fetch_and_publish_assets(self):
        """Fetch assets data and publish to queue"""
        assets_config = self.config.tables['asset_owned']
        
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                # Use config query with limited rows for testing
                assets_query = assets_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 5 ROWS ONLY")
                
                cursor.execute(assets_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"🏢 Fetched {len(rows)} asset records")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.assets_processor.process_record(row, 'asset_owned')
                    if self.assets_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'asset_owned_queue')
                    self.logger.info(f"✅ Published {len(records)} asset records")
                
        except Exception as e:
            self.logger.error(f"❌ Assets fetch error: {e}")
    
    def fetch_and_publish_bot_balances(self):
        """Fetch BOT balances data and publish to queue"""
        bot_config = self.config.tables['balances_bot']
        
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                # Use config query (already has FETCH FIRST 1000 ROWS ONLY)
                cursor.execute(bot_config.query)
                rows = cursor.fetchall()
                
                self.logger.info(f"🏛️ Fetched {len(rows)} BOT balance records")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.bot_balances_processor.process_record(row, 'balances_bot')
                    if self.bot_balances_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'balances_bot_queue')
                    self.logger.info(f"✅ Published {len(records)} BOT balance records")
                
        except Exception as e:
            self.logger.error(f"❌ BOT balances fetch error: {e}")
    
    def fetch_and_publish_mnos(self):
        """Fetch MNOs balances data and publish to queue"""
        mnos_config = self.config.tables['balances_with_mnos']
        
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                # Use config query with limited rows for testing
                mnos_query = mnos_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 5 ROWS ONLY")
                
                cursor.execute(mnos_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"📱 Fetched {len(rows)} MNOs balance records")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.mnos_processor.process_record(row, 'balances_with_mnos')
                    if self.mnos_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'balances_with_mnos_queue')
                    self.logger.info(f"✅ Published {len(records)} MNOs balance records")
                
        except Exception as e:
            self.logger.error(f"❌ MNOs balances fetch error: {e}")
    
    def fetch_and_publish_other_banks(self):
        """Fetch Other Banks balances data and publish to queue"""
        other_banks_config = self.config.tables['balance_with_other_banks']
        
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                # Use config query with limited rows for testing
                other_banks_query = other_banks_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 5 ROWS ONLY")
                
                cursor.execute(other_banks_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"🏦 Fetched {len(rows)} Other Banks balance records")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.other_banks_processor.process_record(row, 'balance_with_other_banks')
                    if self.other_banks_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'balance_with_other_banks_queue')
                    self.logger.info(f"✅ Published {len(records)} Other Banks balance records")
                
        except Exception as e:
            self.logger.error(f"❌ Other Banks balances fetch error: {e}")
    
    def fetch_and_publish_other_assets(self):
        """Fetch Other Assets data and publish to queue"""
        other_assets_config = self.config.tables['other_assets']
        
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                # Use config query with limited rows for testing
                other_assets_query = other_assets_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 10 ROWS ONLY")
                
                cursor.execute(other_assets_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"💎 Fetched {len(rows)} Other Assets records")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.other_assets_processor.process_record(row, 'other_assets')
                    if self.other_assets_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'other_assets_queue')
                    self.logger.info(f"✅ Published {len(records)} Other Assets records")
                
        except Exception as e:
            self.logger.error(f"❌ Other Assets fetch error: {e}")
    
    def fetch_and_publish_overdraft(self):
        """Fetch Overdraft data and publish to queue"""
        overdraft_config = self.config.tables['overdraft']
        
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                # Use config query with limited rows for testing
                overdraft_query = overdraft_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 10 ROWS ONLY")
                
                cursor.execute(overdraft_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"💳 Fetched {len(rows)} Overdraft records")
                
                # Process and publish
                records = []
                for row in rows:
                    record = self.overdraft_processor.process_record(row, 'overdraft')
                    if self.overdraft_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_records(records, 'overdraft_queue')
                    self.logger.info(f"✅ Published {len(records)} Overdraft records")
                
        except Exception as e:
            self.logger.error(f"❌ Overdraft fetch error: {e}")
    
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
            
            def process_cash_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = CashRecord(**record_data)
                    
                    self.logger.info(f"💰 Processing cash: Branch {record.branch_code}, {record.currency}, {record.amount_local:,.2f}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.cash_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ Cash record inserted")
                    
                except Exception as e:
                    self.logger.error(f"❌ Cash processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='cash_information_queue',
                on_message_callback=process_cash_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Cash consumer error: {e}")
    
    def consume_assets_queue(self):
        """Consume assets records from queue"""
        self.logger.info("🏢 Starting assets consumer...")
        
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
            
            def process_assets_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = AssetsRecord(**record_data)
                    
                    self.logger.info(f"🏢 Processing asset: {record.asset_type}, {record.org_cost_value:,.2f} {record.currency}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.assets_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ Asset record inserted")
                    
                except Exception as e:
                    self.logger.error(f"❌ Assets processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='asset_owned_queue',
                on_message_callback=process_assets_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Assets consumer error: {e}")
    
    def consume_bot_balances_queue(self):
        """Consume BOT balances records from queue"""
        self.logger.info("🏛️ Starting BOT balances consumer...")
        
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
            
            def process_bot_balances_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = BotBalancesRecord(**record_data)
                    
                    self.logger.info(f"🏛️ Processing BOT balance: Account {record.account_number}, {record.org_amount:,.2f} {record.currency}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.bot_balances_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ BOT balance record inserted")
                    
                except Exception as e:
                    self.logger.error(f"❌ BOT balances processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='balances_bot_queue',
                on_message_callback=process_bot_balances_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ BOT balances consumer error: {e}")
    
    def consume_mnos_queue(self):
        """Consume MNOs balances records from queue"""
        self.logger.info("📱 Starting MNOs balances consumer...")
        
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
            
            def process_mnos_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = MnosRecord(**record_data)
                    
                    self.logger.info(f"📱 Processing MNOs: {record.mno_code}, Till {record.till_number}, {record.org_float_amount:,.2f} {record.currency}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.mnos_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ MNOs balance record inserted")
                    
                except Exception as e:
                    self.logger.error(f"❌ MNOs processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='balances_with_mnos_queue',
                on_message_callback=process_mnos_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ MNOs balances consumer error: {e}")
    
    def consume_other_banks_queue(self):
        """Consume Other Banks balances records from queue"""
        self.logger.info("🏦 Starting Other Banks balances consumer...")
        
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
            
            def process_other_banks_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = OtherBanksRecord(**record_data)
                    
                    self.logger.info(f"🏦 Processing Other Bank: {record.account_name}, Account {record.account_number}, {record.org_amount:,.2f} {record.currency}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.other_banks_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ Other Banks balance record inserted")
                    
                except Exception as e:
                    self.logger.error(f"❌ Other Banks processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='balance_with_other_banks_queue',
                on_message_callback=process_other_banks_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Other Banks balances consumer error: {e}")
    
    def consume_other_assets_queue(self):
        """Consume Other Assets records from queue"""
        self.logger.info("💎 Starting Other Assets consumer...")
        
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
            
            def process_other_assets_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = OtherAssetsRecord(**record_data)
                    
                    self.logger.info(f"💎 Processing Other Asset: {record.asset_type}, Debtor: {record.debtor_name}, {record.org_amount:,.2f} {record.currency}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.other_assets_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ Other Assets record inserted")
                    
                except Exception as e:
                    self.logger.error(f"❌ Other Assets processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='other_assets_queue',
                on_message_callback=process_other_assets_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Other Assets consumer error: {e}")
    
    def consume_overdraft_queue(self):
        """Consume Overdraft records from queue"""
        self.logger.info("💳 Starting Overdraft consumer...")
        
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
            
            def process_overdraft_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = OverdraftRecord(**record_data)
                    
                    self.logger.info(f"💳 Processing Overdraft: Account {record.account_number}, Client: {record.client_name}, {record.org_sanctioned_amount:,.2f} {record.currency}")
                    
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.overdraft_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ Overdraft record inserted")
                    
                except Exception as e:
                    self.logger.error(f"❌ Overdraft processing error: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue='overdraft_queue',
                on_message_callback=process_overdraft_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Overdraft consumer error: {e}")
    
    def fetch_and_publish_branch(self):
        """Fetch Branch data and publish to queue"""
        branch_config = self.config.tables['branch']
        
        try:
            with self.get_db2_connection() as db2_conn:
                cursor = db2_conn.cursor()
                cursor.execute(branch_config.query)
                
                records = []
                for row in cursor.fetchall():
                    record = self.branch_processor.process_record(row, 'branch')
                    if self.branch_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_to_queue(records, 'branch_queue')
                    self.logger.info(f"🏢 Published {len(records)} branch records")
                else:
                    self.logger.info("🏢 No branch records to publish")
                    
        except Exception as e:
            self.logger.error(f"❌ Branch fetch error: {e}")
    
    def consume_branch_queue(self):
        """Consume Branch records from queue"""
        self.logger.info("🏢 Starting Branch consumer...")
        
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
            
            def process_branch_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = BranchRecord(**record_data)
                    
                    with self.get_postgres_connection() as pg_conn:
                        cursor = pg_conn.cursor()
                        self.branch_processor.insert_to_postgres(record, cursor)
                        pg_conn.commit()
                        cursor.close()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ Processed branch: {record.branch_code}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Branch processing error: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_consume(
                queue='branch_queue',
                on_message_callback=process_branch_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Branch consumer error: {e}")
    
    def fetch_and_publish_agents(self):
        """Fetch Agents data and publish to queue"""
        agents_config = self.config.tables['agents']
        
        try:
            with self.get_db2_connection() as db2_conn:
                cursor = db2_conn.cursor()
                cursor.execute(agents_config.query)
                
                records = []
                for row in cursor.fetchall():
                    record = self.agent_processor.process_record(row, 'agents')
                    if self.agent_processor.validate_record(record):
                        records.append(record)
                
                if records:
                    self.publish_to_queue(records, 'agents_queue')
                    self.logger.info(f"👥 Published {len(records)} agent records")
                else:
                    self.logger.info("👥 No agent records to publish")
                    
        except Exception as e:
            self.logger.error(f"❌ Agents fetch error: {e}")
    
    def consume_agents_queue(self):
        """Consume Agents records from queue"""
        self.logger.info("👥 Starting Agents consumer...")
        
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
            
            def process_agents_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = AgentRecord(**record_data)
                    
                    with self.get_postgres_connection() as pg_conn:
                        cursor = pg_conn.cursor()
                        self.agent_processor.insert_to_postgres(record, cursor)
                        pg_conn.commit()
                        cursor.close()
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(f"✅ Processed agent: {record.agent_id}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Agents processing error: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_consume(
                queue='agents_queue',
                on_message_callback=process_agents_message
            )
            
            # Process messages for a short time
            start_time = time.time()
            while time.time() - start_time < 30 and self.running:  # Run for 30 seconds
                connection.process_data_events(time_limit=1)
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"❌ Agents consumer error: {e}")
    
    def run_test(self):
        """Run a test of the multi-table pipeline"""
        self.logger.info("🚀 Starting Multi-table Pipeline Test")
        self.logger.info("=" * 50)
        
        try:
            # Step 1: Setup queues
            self.setup_rabbitmq_queues()
            
            # Step 2: Fetch and publish data
            self.logger.info("📊 Fetching and publishing data...")
            self.fetch_and_publish_cash()
            self.fetch_and_publish_assets()
            self.fetch_and_publish_bot_balances()
            self.fetch_and_publish_mnos()
            self.fetch_and_publish_other_banks()
            self.fetch_and_publish_other_assets()
            self.fetch_and_publish_overdraft()
            self.fetch_and_publish_branch()
            self.fetch_and_publish_agents()
            
            # Step 3: Start consumers in threads
            self.logger.info("🔄 Starting consumers...")
            
            cash_thread = threading.Thread(target=self.consume_cash_queue, daemon=True)
            assets_thread = threading.Thread(target=self.consume_assets_queue, daemon=True)
            bot_balances_thread = threading.Thread(target=self.consume_bot_balances_queue, daemon=True)
            mnos_thread = threading.Thread(target=self.consume_mnos_queue, daemon=True)
            other_banks_thread = threading.Thread(target=self.consume_other_banks_queue, daemon=True)
            other_assets_thread = threading.Thread(target=self.consume_other_assets_queue, daemon=True)
            overdraft_thread = threading.Thread(target=self.consume_overdraft_queue, daemon=True)
            branch_thread = threading.Thread(target=self.consume_branch_queue, daemon=True)
            agents_thread = threading.Thread(target=self.consume_agents_queue, daemon=True)
            
            cash_thread.start()
            assets_thread.start()
            bot_balances_thread.start()
            mnos_thread.start()
            other_banks_thread.start()
            other_assets_thread.start()
            overdraft_thread.start()
            branch_thread.start()
            agents_thread.start()
            
            # Wait for consumers to process
            self.logger.info("⏳ Processing for 35 seconds...")
            time.sleep(35)
            
            self.running = False
            
            # Wait for threads to finish
            cash_thread.join(timeout=5)
            assets_thread.join(timeout=5)
            bot_balances_thread.join(timeout=5)
            mnos_thread.join(timeout=5)
            other_banks_thread.join(timeout=5)
            other_assets_thread.join(timeout=5)
            overdraft_thread.join(timeout=5)
            
            self.logger.info("✅ Multi-table pipeline test completed!")
            
            # Check final results
            from check_data import check_data
            check_data()
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline test failed: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    pipeline = SimpleMultiPipeline()
    pipeline.run_test()