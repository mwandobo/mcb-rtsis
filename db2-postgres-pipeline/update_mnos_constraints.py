#!/usr/bin/env python3
"""
Update balanceWithMnos table constraints to use composite key
"""

import psycopg2
import logging
from config import Config

def update_mnos_constraints():
    """Update balanceWithMnos table to use composite key (mnoCode + tillNumber)"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔧 UPDATING BALANCE WITH MNOS TABLE CONSTRAINTS")
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
        
        # Drop the existing unique constraint on mnoCode only
        logger.info("🗑️ Dropping existing unique constraint on mnoCode...")
        pg_cursor.execute('DROP INDEX IF EXISTS idx_balance_mnos_unique')
        
        # Create new composite unique constraint on (mnoCode, tillNumber)
        logger.info("🔧 Creating composite unique constraint on (mnoCode, tillNumber)...")
        pg_cursor.execute("""
            CREATE UNIQUE INDEX idx_balance_mnos_composite_unique 
            ON "balanceWithMnos" ("mnoCode", "tillNumber")
        """)
        
        pg_conn.commit()
        
        # Verify the changes
        pg_cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes 
            WHERE tablename = 'balanceWithMnos' 
            AND indexname LIKE '%unique%'
        """)
        
        unique_indexes = pg_cursor.fetchall()
        logger.info("📋 Unique indexes:")
        for idx_name, idx_def in unique_indexes:
            logger.info(f"  - {idx_name}: {idx_def}")
        
        # Close connections
        pg_cursor.close()
        pg_conn.close()
        
        logger.info("✅ Constraint update completed successfully!")
        logger.info("📋 Now each MNO can have multiple records with different till numbers")
        
    except Exception as e:
        logger.error(f"❌ Error updating constraints: {e}")
        raise

if __name__ == "__main__":
    update_mnos_constraints()