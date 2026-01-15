#!/usr/bin/env python3
"""
Delete balanceWithMnos data
"""

import psycopg2
import logging
from config import Config

def delete_mnos_data():
    """Delete all balanceWithMnos data"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🗑️ DELETING BALANCE WITH MNOS DATA")
    logger.info("=" * 50)
    
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
        
        # Delete all records
        pg_cursor.execute('DELETE FROM "balanceWithMnos"')
        deleted_count = pg_cursor.rowcount
        pg_conn.commit()
        
        logger.info(f"🗑️ Deleted {deleted_count} records from balanceWithMnos")
        
        # Verify deletion
        pg_cursor.execute('SELECT COUNT(*) FROM "balanceWithMnos"')
        remaining_count = pg_cursor.fetchone()[0]
        logger.info(f"📊 Remaining records: {remaining_count}")
        
        # Close connections
        pg_cursor.close()
        pg_conn.close()
        
        logger.info("✅ Deletion completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error deleting data: {e}")
        raise

if __name__ == "__main__":
    delete_mnos_data()