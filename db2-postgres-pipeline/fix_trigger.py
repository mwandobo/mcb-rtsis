#!/usr/bin/env python3
"""
Fix the trigger function to use correct column name (updated_at instead of updatedAt)
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_trigger():
    """Fix trigger function"""
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
        
        logger.info("Checking which tables use the trigger function...")
        
        # Get all triggers using this function
        cursor.execute("""
            SELECT event_object_table, trigger_name
            FROM information_schema.triggers
            WHERE action_statement LIKE '%update_updated_at_column%'
        """)
        
        affected_tables = cursor.fetchall()
        logger.info(f"Found {len(affected_tables)} tables using this trigger:")
        for table, trigger in affected_tables:
            logger.info(f"  - {table}: {trigger}")
        
        logger.info("\nDropping all triggers...")
        
        # Drop all triggers
        for table, trigger in affected_tables:
            cursor.execute(f'DROP TRIGGER IF EXISTS {trigger} ON "{table}"')
            logger.info(f"  ✓ Dropped trigger {trigger} from {table}")
        
        logger.info("\nDropping old function...")
        
        # Drop the function
        cursor.execute("""
            DROP FUNCTION IF EXISTS update_updated_at_column()
        """)
        
        logger.info("Creating new trigger function with correct column name...")
        
        # Create the corrected function
        cursor.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        logger.info("\nRecreating all triggers...")
        
        # Recreate all triggers
        for table, trigger in affected_tables:
            cursor.execute(f"""
                CREATE TRIGGER {trigger}
                BEFORE UPDATE ON "{table}"
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            """)
            logger.info(f"  ✓ Created trigger {trigger} on {table}")
        
        conn.commit()
        
        logger.info("\n✓ Trigger function fixed successfully")
        logger.info("  - Function now uses: NEW.updated_at (not NEW.\"updatedAt\")")
        logger.info(f"  - Recreated {len(affected_tables)} triggers")
        
        # Verify
        cursor.execute("""
            SELECT prosrc 
            FROM pg_proc 
            WHERE proname = 'update_updated_at_column'
        """)
        
        result = cursor.fetchone()
        if result:
            logger.info("\nNew trigger function source:")
            logger.info("-" * 60)
            logger.info(result[0])
            logger.info("-" * 60)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("FIXING TRIGGER FUNCTION")
    logger.info("=" * 60)
    fix_trigger()
    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)
