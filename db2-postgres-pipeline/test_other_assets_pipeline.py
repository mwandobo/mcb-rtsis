#!/usr/bin/env python3
"""
Test script for Other Assets pipeline
"""

import sys
import os
import logging
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.other_assets_processor import OtherAssetsProcessor
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_other_assets_pipeline():
    """Test the Other Assets data pipeline"""
    config = Config()
    
    # Get table configuration
    table_config = config.tables['other_assets']
    
    logger.info("Starting Other Assets pipeline test...")
    logger.info(f"Query: {table_config.query[:100]}...")
    
    # Initialize connections
    db2_conn = None
    pg_conn = None
    
    try:
        # Initialize connections
        logger.info("Connecting to DB2...")
        db2_conn = DB2Connection()
        
        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        
        # Initialize processor
        processor = OtherAssetsProcessor()
        
        # Execute DB2 query
        logger.info("Executing DB2 query...")
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(table_config.query)
        
            # Fetch and process records
            records_processed = 0
            records_inserted = 0
            
            while True:
                rows = cursor.fetchmany(100)  # Process in batches
                if not rows:
                    break
                
            for row in rows:
                try:
                    # Process the record
                    record = processor.process_record(row, table_config.name)
                    
                    # Validate the record
                    if processor.validate_record(record):
                        # Insert to PostgreSQL
                        processor.insert_to_postgres(record, pg_cursor)
                        records_inserted += 1
                        
                        # Log sample record
                        if records_inserted <= 3:
                            logger.info(f"Sample record {records_inserted}:")
                            logger.info(f"  Asset Type: {record.asset_type}")
                            logger.info(f"  Transaction Date: {record.transaction_date}")
                            logger.info(f"  Debtor Name: {record.debtor_name}")
                            logger.info(f"  Currency: {record.currency}")
                            logger.info(f"  Org Amount: {record.org_amount}")
                            logger.info(f"  TZS Amount: {record.tzs_amount}")
                            logger.info(f"  Sector Classification: {record.sector_sna_classification}")
                    else:
                        logger.warning(f"Invalid record skipped: {record}")
                        
                    records_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing record: {e}")
                    logger.error(f"Raw data: {row}")
                    continue
        
            # Commit PostgreSQL transaction
            pg_conn.commit()
            
            logger.info(f"Pipeline test completed successfully!")
            logger.info(f"Records processed: {records_processed}")
            logger.info(f"Records inserted: {records_inserted}")
        
            # Verify data in PostgreSQL
            logger.info("Verifying data in PostgreSQL...")
            pg_cursor.execute("SELECT COUNT(*) FROM other_assets")
            total_count = pg_cursor.fetchone()[0]
            logger.info(f"Total records in other_assets table: {total_count}")
            
            # Show sample data
            pg_cursor.execute("""
                SELECT asset_type, transaction_date, debtor_name, currency, org_amount, tzs_amount
                FROM other_assets 
                ORDER BY transaction_date DESC 
                LIMIT 5
            """)
            
            sample_records = pg_cursor.fetchall()
            logger.info("Sample records from PostgreSQL:")
            for i, record in enumerate(sample_records, 1):
                logger.info(f"  {i}. Asset Type: {record[0]}, Date: {record[1]}, "
                           f"Debtor: {record[2]}, Currency: {record[3]}, "
                           f"Amount: {record[4]}, TZS: {record[5]}")
        
    except Exception as e:
        logger.error(f"Pipeline test failed: {e}")
        if pg_conn:
            pg_conn.rollback()
        raise
        
    finally:
        # Close connections
        if pg_conn:
            pg_conn.close()
        
        logger.info("Connections closed.")

if __name__ == "__main__":
    test_other_assets_pipeline()