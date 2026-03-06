#!/usr/bin/env python3
"""
Drop loanInformation table from PostgreSQL
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def drop_loans_table():
    """Drop the loanInformation table"""
    
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
        logger.info("Dropping loanInformation table...")
        cursor.execute('DROP TABLE IF EXISTS "loanInformation" CASCADE')
        
        conn.commit()
        
        logger.info("loanInformation table dropped successfully!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error dropping loanInformation table: {e}")
        raise

if __name__ == "__main__":
    drop_loans_table()
