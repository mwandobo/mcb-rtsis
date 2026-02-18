#!/usr/bin/env python3
"""
Check agents table count
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_count():
    """Check agents count"""
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
        
        # Get count
        cursor.execute('SELECT COUNT(*) FROM "agents"')
        count = cursor.fetchone()[0]
        
        logger.info(f"Total records in agents table: {count:,}")
        
        # Get sample records
        cursor.execute('SELECT "agentId", "agentName", "terminalID" FROM "agents" LIMIT 5')
        samples = cursor.fetchall()
        
        logger.info("\nSample records:")
        for agent_id, agent_name, terminal_id in samples:
            logger.info(f"  - {agent_id}: {agent_name} (Terminal: {terminal_id})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_count()
