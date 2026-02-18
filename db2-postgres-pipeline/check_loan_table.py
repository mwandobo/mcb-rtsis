"""
Check if loanTransaction table exists
"""

import logging
from config import Config
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_table():
    """Check for loanTransaction table"""
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
        
        # Check for tables with 'loan' in the name
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE '%loan%'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        logger.info(f"Found {len(tables)} tables with 'loan' in name:")
        for table in tables:
            logger.info(f"  - {table[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    check_table()
