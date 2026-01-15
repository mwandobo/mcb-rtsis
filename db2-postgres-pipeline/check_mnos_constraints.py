#!/usr/bin/env python3
"""
Check balanceWithMnos table constraints
"""

import psycopg2
import logging
from config import Config

def check_mnos_constraints():
    """Check balanceWithMnos table constraints and indexes"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 CHECKING BALANCE WITH MNOS TABLE CONSTRAINTS")
    logger.info("=" * 60)
    
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
        
        # Check table structure
        pg_cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithMnos' 
            ORDER BY ordinal_position
        """)
        
        columns = pg_cursor.fetchall()
        logger.info("📋 Table structure:")
        for col_name, data_type, nullable in columns:
            logger.info(f"  - {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
        
        # Check constraints
        pg_cursor.execute("""
            SELECT conname, contype, pg_get_constraintdef(oid) as definition
            FROM pg_constraint 
            WHERE conrelid = '"balanceWithMnos"'::regclass
        """)
        
        constraints = pg_cursor.fetchall()
        logger.info("📋 Constraints:")
        if constraints:
            for con_name, con_type, definition in constraints:
                logger.info(f"  - {con_name} ({con_type}): {definition}")
        else:
            logger.info("  - No constraints found")
        
        # Check indexes
        pg_cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes 
            WHERE tablename = 'balanceWithMnos'
        """)
        
        indexes = pg_cursor.fetchall()
        logger.info("📋 Indexes:")
        if indexes:
            for idx_name, idx_def in indexes:
                logger.info(f"  - {idx_name}: {idx_def}")
        else:
            logger.info("  - No indexes found")
        
        # Close connections
        pg_cursor.close()
        pg_conn.close()
        
        logger.info("✅ Constraint check completed!")
        
    except Exception as e:
        logger.error(f"❌ Error checking constraints: {e}")
        raise

if __name__ == "__main__":
    check_mnos_constraints()