#!/usr/bin/env python3
"""
Simple count check for deposits data
"""

import psycopg2
from config import Config

def simple_count():
    """Simple count check"""
    
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
        cursor.execute('SELECT COUNT(*) FROM "deposits"')
        count = cursor.fetchone()[0]
        print(f"Deposits Records: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    simple_count()