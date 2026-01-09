#!/usr/bin/env python3
"""
Check table constraints
"""

import psycopg2
from config import Config

def check_constraints():
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
        
        # Check indexes on balanceWithOtherBank table
        cursor.execute("""
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE tablename = 'balanceWithOtherBank'
        """)
        
        indexes = cursor.fetchall()
        print("Indexes on balanceWithOtherBank:")
        for index_name, index_def in indexes:
            print(f"  {index_name}: {index_def}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_constraints()