#!/usr/bin/env python3
"""
Add entityType column to existing personalDataCorporate table
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_entity_type_column():
    config = Config()
    
    logger.info("="*80)
    logger.info("ADDING ENTITY TYPE COLUMN TO PERSONAL DATA CORPORATE TABLE")
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
        
        # Check if column already exists
        logger.info("Checking if entityType column exists...")
        cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'personalDataCorporate' 
        AND column_name = 'entityType'
        """)
        
        if cursor.fetchone():
            logger.info("✓ entityType column already exists")
        else:
            logger.info("Adding entityType column...")
            cursor.execute("""
            ALTER TABLE "personalDataCorporate" 
            ADD COLUMN "entityType" VARCHAR(100)
            """)
            logger.info("✓ entityType column added successfully")
        
        # Check if other missing columns exist and add them
        missing_columns = [
            ('numberOfEmployees', 'INTEGER'),
            ('street', 'VARCHAR(200)'),
            ('country', 'VARCHAR(100)'),
            ('poBox', 'VARCHAR(200)'),
            ('zipCode', 'VARCHAR(50)')
        ]
        
        for column_name, column_type in missing_columns:
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'personalDataCorporate' 
            AND column_name = %s
            """, (column_name,))
            
            if cursor.fetchone():
                logger.info(f"✓ {column_name} column already exists")
            else:
                logger.info(f"Adding {column_name} column...")
                if column_name in ['street', 'country', 'poBox', 'zipCode']:
                    cursor.execute(f'ALTER TABLE "personalDataCorporate" ADD COLUMN {column_name} {column_type}')
                else:
                    cursor.execute(f'ALTER TABLE "personalDataCorporate" ADD COLUMN "{column_name}" {column_type}')
                logger.info(f"✓ {column_name} column added successfully")
        
        conn.commit()
        logger.info("="*80)
        logger.info("✓ All missing columns have been added successfully")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    add_entity_type_column()