#!/usr/bin/env python3
"""
BOT Balances Pipeline - BOT Project
Migrates BOT balances data from DB2 to PostgreSQL based on balances-bot.sql
"""

import sys
import os
import logging
from datetime import datetime
import time

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.bot_balances_processor import BotBalancesProcessor
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BotBalancesPipeline:
    """BOT balances pipeline for migrating data from DB2 to PostgreSQL"""
    
    def __init__(self, batch_size=1000, resume_mode=True):
        self.batch_size = batch_size
        self.resume_mode = resume_mode
        self.processor = BotBalancesProcessor()
        self.config = Config()
        # Read the BOT balances query from the SQL file
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sqls', 'balances-bot.sql')
        with open(sql_file_path, 'r') as f:
            self.bot_balances_query = f.read()
        self.total_processed = 0
        self.total_new_records = 0
        self.total_skipped = 0
        self.last_processed_id = None
        
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        pg_config = {
            'host': self.config.database.pg_host,
            'port': self.config.database.pg_port,
            'database': self.config.database.pg_database,
            'user': self.config.database.pg_user,
            'password': self.config.database.pg_password
        }
        return psycopg2.connect(**pg_config)
    
    def get_existing_records_count(self):
        """Get count of existing records in PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT COUNT(*) FROM "balances_bot"')
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.warning(f"Could not get existing records count: {e}")
            return 0
    
    def get_total_records_count(self):
        """Get total count of records available in DB2"""
        try:
            db2_conn = DB2Connection()
            with db2_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Create a simpler count query
                    count_query = """
                    SELECT COUNT(*) 
                    FROM GLI_TRX_EXTRACT AS gte
                    JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                    JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
                    LEFT JOIN CURRENCY cu ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))
                    WHERE gl.EXTERNAL_GLACCOUNT='100028000'
                    """
                    cursor.execute(count_query)
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting total records count: {e}")
            return 0
    
    def get_last_processed_transaction_date(self):
        """Get the last processed transaction date for resume functionality"""
        if not self.resume_mode:
            return None
            
        try:
            with self.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        SELECT MAX("transactionDate") 
                        FROM "balances_bot"
                    ''')
                    result = cursor.fetchone()[0]
                    return result
        except Exception as e:
            logger.warning(f"Could not get last processed date: {e}")
            return None
    
    def process_batch(self, batch_data, batch_num):
        """Process a batch of BOT balances records"""
        new_records = 0
        skipped_records = 0
        
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    
                    for i, raw_record in enumerate(batch_data, 1):
                        try:
                            # Process the record
                            record = self.processor.process_record(raw_record, 'balances_bot')
                            
                            # Validate the record
                            if not self.processor.validate_record(record):
                                logger.warning(f"⚠️ Invalid record skipped: {record.account_number}")
                                skipped_records += 1
                                continue
                            
                            # Insert to PostgreSQL
                            self.processor.insert_to_postgres(record, pg_cursor)
                            new_records += 1
                            
                            # Track last processed
                            self.last_processed_id = record.account_number
                            
                            # Progress logging
                            if i % 100 == 0:
                                logger.info(f"✅ Processed {i} records in batch {batch_num}...")
                                
                        except Exception as e:
                            logger.error(f"❌ Error processing BOT balances record: {str(e)}")
                            skipped_records += 1
                            continue
                    
                    # Commit the batch
                    pg_conn.commit()
                    logger.info(f"✅ Batch {batch_num} committed successfully")
                    
        except Exception as e:
            logger.error(f"❌ Error processing batch {batch_num}: {str(e)}")
            raise
        
        return new_records, skipped_records
    
    def run_complete_pipeline(self):
        """Run the complete BOT balances pipeline"""
        logger.info("🏦 Starting Complete BOT Balances Pipeline")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # Get initial counts
        existing_count = self.get_existing_records_count()
        total_available = self.get_total_records_count()
        last_processed_date = self.get_last_processed_transaction_date()
        
        logger.info(f"📊 Existing records in PostgreSQL: {existing_count}")
        logger.info(f"📊 Total BOT balances records available: {total_available}")
        if last_processed_date:
            logger.info(f"📅 Last processed transaction date: {last_processed_date}")
        
        if total_available == 0:
            logger.warning("⚠️ No BOT balances records found in DB2")
            return
        
        batch_num = 1
        
        try:
            # Connect to DB2 and fetch data
            db2_conn = DB2Connection()
            with db2_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    logger.info(f"\n📊 Batch 1: Fetching BOT balances records...")
                    logger.info("[OK] Connected to DB2")
                    logger.info("📊 Executing BOT balances query...")
                    
                    # Execute the BOT balances query
                    cursor.execute(self.bot_balances_query)
                    
                    # Process records in batches
                    batch_num = 1
                    while True:
                        # Fetch batch data
                        batch_data = cursor.fetchmany(self.batch_size)
                        
                        if not batch_data:
                            logger.info("✅ No more BOT balances records to process")
                            break
                        
                        logger.info(f"🏦 Fetched {len(batch_data)} BOT balances records in batch {batch_num}")
                        
                        # Log sample records for first batch
                        if batch_num == 1:
                            logger.info("📋 Sample BOT balances records:")
                            for i, record in enumerate(batch_data[:3], 1):
                                account_number = record[1] if len(record) > 1 else 'N/A'
                                currency = record[5] if len(record) > 5 else 'N/A'
                                amount = record[6] if len(record) > 6 else 'N/A'
                                logger.info(f"  {i}. Account: {account_number} | Currency: {currency} | Amount: {amount}")
                        
                        # Process the batch
                        new_records, skipped_records = self.process_batch(batch_data, batch_num)
                        
                        # Update totals
                        self.total_processed += len(batch_data)
                        self.total_new_records += new_records
                        self.total_skipped += skipped_records
                        
                        logger.info(f"✅ Batch {batch_num} completed: {new_records} new records, {skipped_records} skipped")
                        logger.info(f"📊 Total processed so far: {self.total_processed}")
                        if self.last_processed_id:
                            logger.info(f"📅 Last processed account: {self.last_processed_id}")
                        
                        # Prepare for next batch
                        batch_num += 1
                        
                        # Small delay between batches
                        time.sleep(1)
                        
        except Exception as e:
            logger.error(f"❌ Pipeline error: {str(e)}")
            raise
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("\n" + "=" * 60)
        logger.info("🎉 BOT Balances Pipeline Completed Successfully!")
        logger.info(f"📊 Total records processed: {self.total_processed}")
        logger.info(f"✅ New records inserted: {self.total_new_records}")
        logger.info(f"⚠️ Records skipped: {self.total_skipped}")
        logger.info(f"⏱️ Total duration: {duration}")
        logger.info("=" * 60)

def main():
    """Main function"""
    print("🏦 BOT Balances Pipeline - BOT Project")
    print("=" * 50)
    
    try:
        # Initialize and run pipeline
        pipeline = BotBalancesPipeline(batch_size=1000, resume_mode=True)
        logger.info("🏦 BOT Balances Pipeline initialized")
        logger.info(f"📊 Batch size: {pipeline.batch_size}")
        logger.info(f"🔄 Resume mode: {pipeline.resume_mode}")
        
        # Run the complete pipeline
        pipeline.run_complete_pipeline()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()