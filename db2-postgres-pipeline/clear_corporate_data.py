#!/usr/bin/env python3
"""
Clear corporate data from PostgreSQL
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_corporate_data():
    config = Config()
    
    logger.info("="*80)
    logger.info("CLEARING CORPORATE DATA FROM POSTGRESQL")
    logger.info("="*80)
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Get current count
        cursor.execute('SELECT COUNT(*) FROM "personalDataCorporate"')
        current_count = cursor.fetchone()[0]
        logger.info(f"Current records in personalDataCorporate: {current_count:,}")
        
        # Clear data
        logger.info("Clearing all records...")
        cursor.execute('DELETE FROM "personalDataCorporate"')
        
        conn.commit()
        
        # Verify cleared
        cursor.execute('SELECT COUNT(*) FROM "personalDataCorporate"')
        final_count = cursor.fetchone()[0]
        
        logger.info(f"✓ Cleared {current_count:,} records")
        logger.info(f"✓ Final count: {final_count:,}")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    clear_corporate_data()