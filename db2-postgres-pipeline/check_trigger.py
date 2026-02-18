#!/usr/bin/env python3
"""
Check the trigger function for agents table
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_trigger():
    """Check trigger function"""
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
        
        # Get trigger function source
        cursor.execute("""
            SELECT prosrc 
            FROM pg_proc 
            WHERE proname = 'update_updated_at_column'
        """)
        
        result = cursor.fetchone()
        if result:
            logger.info("Trigger function 'update_updated_at_column' source:")
            logger.info("-" * 60)
            logger.info(result[0])
            logger.info("-" * 60)
        else:
            logger.info("Trigger function 'update_updated_at_column' not found")
        
        # Check triggers on agents table
        cursor.execute("""
            SELECT trigger_name, event_manipulation, action_timing
            FROM information_schema.triggers
            WHERE event_object_table = 'agents'
        """)
        
        triggers = cursor.fetchall()
        if triggers:
            logger.info("\nTriggers on 'agents' table:")
            for trigger_name, event, timing in triggers:
                logger.info(f"  - {trigger_name}: {timing} {event}")
        else:
            logger.info("\nNo triggers found on 'agents' table")
        
        # Check column names
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'agents' 
            AND column_name LIKE '%update%'
        """)
        
        columns = cursor.fetchall()
        if columns:
            logger.info("\nColumns with 'update' in name:")
            for col in columns:
                logger.info(f"  - {col[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_trigger()
