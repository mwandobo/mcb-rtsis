#!/usr/bin/env python3
"""
Branch Information Pipeline - BOT Project
Migrates branch data from DB2 to PostgreSQL based on branch.sql
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
from processors.branch_processor import BranchProcessor
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BranchPipeline:
    """Branch information pipeline for migrating data from DB2 to PostgreSQL"""
    
    def __init__(self, batch_size=500):
        self.batch_size = batch_size
        self.processor = BranchProcessor()
        self.config = Config()
        # Read the branch query from the SQL file (go up one directory)
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sqls', 'branch.sql')
        with open(sql_file_path, 'r') as f:
            self.branch_query = f.read()
        self.total_processed = 0
        self.total_new_records = 0
        self.total_skipped = 0
        
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
                    cursor.execute('SELECT COUNT(*) FROM "branch"')
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
                    FROM UNIT u
                    WHERE u.UNIT_NAME = 'MLIMANI BRANCH'
                       OR u.UNIT_NAME = 'SAMORA BRANCH'
                    """
                    cursor.execute(count_query)
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting total records count: {e}")
            return 0
    
    def process_batch(self, batch_data, batch_num):
        """Process a batch of branch records"""
        new_records = 0
        skipped_records = 0
        
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    
                    for i, raw_record in enumerate(batch_data, 1):
                        try:
                            # Process the record
                            record = self.processor.process_record(raw_record, 'branch')
                            
                            # Validate the record
                            if not self.processor.validate_record(record):
                                logger.warning(f"⚠️ Invalid record skipped: {record.branch_code}")
                                skipped_records += 1
                                continue
                            
                            # Insert to PostgreSQL
                            self.processor.insert_to_postgres(record, pg_cursor)
                            new_records += 1
                            
                            # Progress logging
                            if i % 100 == 0:
                                logger.info(f"✅ Processed {i} records in batch {batch_num}...")
                                
                        except Exception as e:
                            logger.error(f"❌ Error processing branch record: {str(e)}")
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
        """Run the complete branch pipeline"""
        logger.info("🏢 Starting Complete Branch Information Pipeline")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # Get initial counts
        existing_count = self.get_existing_records_count()
        total_available = self.get_total_records_count()
        
        logger.info(f"📊 Existing records in PostgreSQL: {existing_count}")
        logger.info(f"📊 Total branch records available: {total_available}")
        
        if total_available == 0:
            logger.warning("⚠️ No branch records found in DB2")
            return
        
        batch_num = 1
        
        try:
            # Connect to DB2 and fetch data
            db2_conn = DB2Connection()
            with db2_conn.get_connection() as conn:
                with conn.cursor() as cursor:
                    
                    logger.info(f"\n📊 Batch {batch_num}: Fetching branch records...")
                    logger.info("[OK] Connected to DB2")
                    logger.info("📊 Executing branch query...")
                    
                    # Execute the branch query
                    cursor.execute(self.branch_query)
                    
                    # Fetch all records (branch data is typically small)
                    batch_data = cursor.fetchall()
                    
                    if not batch_data:
                        logger.info("✅ No new branch records to process")
                        return
                    
                    logger.info(f"🏢 Fetched {len(batch_data)} branch records")
                    
                    # Log sample records
                    logger.info("📋 Sample branch records:")
                    for i, record in enumerate(batch_data[:3], 1):
                        branch_name = record[1] if len(record) > 1 else 'N/A'
                        branch_code = record[4] if len(record) > 4 else 'N/A'
                        status = record[16] if len(record) > 16 else 'N/A'
                        logger.info(f"  {i}. Branch: {branch_code} | Name: {branch_name} | Status: {status}")
                    
                    # Process the batch
                    new_records, skipped_records = self.process_batch(batch_data, batch_num)
                    
                    # Update totals
                    self.total_processed += len(batch_data)
                    self.total_new_records += new_records
                    self.total_skipped += skipped_records
                    
                    logger.info(f"✅ Batch {batch_num} completed: {new_records} new records, {skipped_records} skipped")
                    logger.info(f"📊 Total processed so far: {self.total_processed}")
                    
        except Exception as e:
            logger.error(f"❌ Pipeline error: {str(e)}")
            raise
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("\n" + "=" * 60)
        logger.info("🎉 Branch Pipeline Completed Successfully!")
        logger.info(f"📊 Total records processed: {self.total_processed}")
        logger.info(f"✅ New records inserted: {self.total_new_records}")
        logger.info(f"⚠️ Records skipped: {self.total_skipped}")
        logger.info(f"⏱️ Total duration: {duration}")
        logger.info("=" * 60)

def main():
    """Main function"""
    print("🏢 Branch Information Pipeline - BOT Project")
    print("=" * 50)
    
    try:
        # Initialize and run pipeline
        pipeline = BranchPipeline(batch_size=500)
        logger.info("🏢 Branch Information Pipeline initialized")
        logger.info(f"📊 Batch size: {pipeline.batch_size}")
        
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