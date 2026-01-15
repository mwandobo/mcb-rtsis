#!/usr/bin/env python3
"""
Update balanceWithMnos table schema to use TIMESTAMP for date fields
"""

import psycopg2
import logging
from config import Config

def update_mnos_table_schema():
    """Update balanceWithMnos table to use TIMESTAMP for date fields"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔧 UPDATING BALANCE WITH MNOS TABLE SCHEMA")
    logger.info("=" * 60)
    
    try:
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        logger.info("✅ Connected to PostgreSQL")
        
        # Check current schema
        pg_cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithMnos' 
            AND column_name IN ('reportingDate', 'floatBalanceDate')
            ORDER BY column_name
        """)
        
        current_schema = pg_cursor.fetchall()
        logger.info("📋 Current schema:")
        for col_name, data_type in current_schema:
            logger.info(f"  - {col_name}: {data_type}")
        
        # Update reportingDate to TIMESTAMP
        logger.info("🔧 Updating reportingDate to TIMESTAMP...")
        pg_cursor.execute("""
            ALTER TABLE "balanceWithMnos" 
            ALTER COLUMN "reportingDate" TYPE TIMESTAMP USING "reportingDate"::TIMESTAMP
        """)
        
        # Update floatBalanceDate to TIMESTAMP
        logger.info("🔧 Updating floatBalanceDate to TIMESTAMP...")
        pg_cursor.execute("""
            ALTER TABLE "balanceWithMnos" 
            ALTER COLUMN "floatBalanceDate" TYPE TIMESTAMP USING "floatBalanceDate"::TIMESTAMP
        """)
        
        pg_conn.commit()
        
        # Verify the changes
        pg_cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithMnos' 
            AND column_name IN ('reportingDate', 'floatBalanceDate')
            ORDER BY column_name
        """)
        
        updated_schema = pg_cursor.fetchall()
        logger.info("📋 Updated schema:")
        for col_name, data_type in updated_schema:
            logger.info(f"  - {col_name}: {data_type}")
        
        # Close connections
        pg_cursor.close()
        pg_conn.close()
        
        logger.info("✅ Schema update completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error updating schema: {e}")
        raise

if __name__ == "__main__":
    update_mnos_table_schema()