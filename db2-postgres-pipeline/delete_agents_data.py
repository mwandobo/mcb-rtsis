#!/usr/bin/env python3
"""
Delete all agents data
"""

import psycopg2
import logging
from config import Config

def delete_agents_data():
    """Delete all agents data from PostgreSQL"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        logger.info("🗑️ DELETING AGENTS DATA")
        logger.info("=" * 50)
        
        # Check current count
        cursor.execute('SELECT COUNT(*) FROM "agents"')
        current_count = cursor.fetchone()[0]
        logger.info(f"📊 Current agents records: {current_count}")
        
        # Delete all records
        logger.info("🗑️ Deleting all agents records...")
        cursor.execute('DELETE FROM "agents"')
        deleted_count = cursor.rowcount
        
        # Commit the deletion
        conn.commit()
        
        # Verify deletion
        cursor.execute('SELECT COUNT(*) FROM "agents"')
        remaining_count = cursor.fetchone()[0]
        
        logger.info(f"✅ Deleted {deleted_count} agents records")
        logger.info(f"📊 Remaining records: {remaining_count}")
        
        if remaining_count == 0:
            logger.info("✅ All agents data successfully deleted!")
        else:
            logger.warning(f"⚠️ Warning: {remaining_count} records still remain")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error deleting agents data: {e}")
        raise

if __name__ == "__main__":
    delete_agents_data()