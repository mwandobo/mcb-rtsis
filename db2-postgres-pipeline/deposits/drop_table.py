#!/usr/bin/env python3
"""
Drop deposits table
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def drop_deposits_table():
    """Drop deposits table"""
    config = Config()
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        logger.info("Dropping deposits table...")
        cursor.execute('DROP TABLE IF EXISTS "deposits" CASCADE')
        conn.commit()
        
        logger.info("Table dropped successfully!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error dropping deposits table: {e}")
        raise


if __name__ == "__main__":
    drop_deposits_table()
