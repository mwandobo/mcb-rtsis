#!/usr/bin/env python3
"""
Add v4 columns to existing personalDataCorporate table
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_v4_columns():
    config = Config()
    
    logger.info("="*80)
    logger.info("ADDING V4 COLUMNS TO PERSONAL DATA CORPORATE TABLE")
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
        
        # Add missing v4 columns
        v4_columns = [
            ('numberOfEmployees', 'INTEGER'),
            ('country', 'VARCHAR(100)'),
            ('poBox', 'VARCHAR(100)'),
            ('zipCode', 'VARCHAR(20)')
        ]
        
        for column_name, column_type in v4_columns:
            try:
                logger.info(f"Adding column {column_name}...")
                cursor.execute(f'ALTER TABLE "personalDataCorporate" ADD COLUMN "{column_name}" {column_type}')
                logger.info(f"✓ Column {column_name} added successfully")
            except psycopg2.errors.DuplicateColumn:
                logger.info(f"Column {column_name} already exists, skipping...")
                conn.rollback()
            except Exception as e:
                logger.error(f"Error adding column {column_name}: {e}")
                conn.rollback()
                raise
        
        conn.commit()
        logger.info("="*80)
        logger.info("V4 COLUMNS ADDED SUCCESSFULLY")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    add_v4_columns()