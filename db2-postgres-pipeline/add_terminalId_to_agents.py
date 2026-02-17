#!/usr/bin/env python3
"""
Add terminalId column to agents table
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_terminal_id_column():
    """Add terminalId column to agents table"""
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
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'agents' 
            AND column_name = 'terminalId'
        """)
        
        if cursor.fetchone():
            logger.info("Column 'terminalId' already exists in agents table")
        else:
            logger.info("Adding 'terminalId' column to agents table...")
            
            # Add the column
            cursor.execute("""
                ALTER TABLE "agents" 
                ADD COLUMN "terminalId" VARCHAR(50)
            """)
            
            conn.commit()
            logger.info("✓ Column 'terminalId' added successfully")
        
        # Show current table structure
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = 'agents'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        logger.info("\nCurrent agents table structure:")
        for col_name, data_type, max_length in columns:
            length_info = f"({max_length})" if max_length else ""
            logger.info(f"  - {col_name}: {data_type}{length_info}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("ADDING terminalId COLUMN TO AGENTS TABLE")
    logger.info("=" * 60)
    add_terminal_id_column()
    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)
