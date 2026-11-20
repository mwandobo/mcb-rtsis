#!/usr/bin/env python3
"""
Drop old cheques table
"""

import psycopg2
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def drop_old_table():
    """Drop the old cheques table"""
    config = Config()
    
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
        cursor.execute('DROP TABLE IF EXISTS "chequeClearing"')
        conn.commit()
        print("✅ Old chequeClearing table dropped")
        conn.close()
        
    except Exception as e:
        print(f"❌ Error dropping old table: {e}")
        sys.exit(1)

if __name__ == "__main__":
    drop_old_table()