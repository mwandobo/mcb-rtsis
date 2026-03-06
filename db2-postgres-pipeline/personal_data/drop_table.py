#!/usr/bin/env python3
"""
Drop personalData table from PostgreSQL
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def drop_personal_data_table():
    """Drop the personalDataInformation table"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Drop table
        logger.info("Dropping personalDataInformation table...")
        cursor.execute('DROP TABLE IF EXISTS "personalDataInformation" CASCADE')
        
        conn.commit()
        
        logger.info("personalDataInformation table dropped successfully!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error dropping personalDataInformation table: {e}")
        raise

if __name__ == "__main__":
    drop_personal_data_table()
