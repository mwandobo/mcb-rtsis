#!/usr/bin/env python3
"""
Clean Income Statement Table - Keep Only One Record
"""

import psycopg2
from config import Config

def clean_income_statement():
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
        
        print("Current state:")
        cursor.execute('SELECT COUNT(*) FROM "incomeStatement"')
        print(f"Total records: {cursor.fetchone()[0]}")
        
        # Clear all records and start fresh
        print("\nClearing all income statement records...")
        cursor.execute('DELETE FROM "incomeStatement"')
        deleted_count = cursor.rowcount
        print(f"Deleted {deleted_count} records")
        
        conn.commit()
        
        print("\nAfter cleanup:")
        cursor.execute('SELECT COUNT(*) FROM "incomeStatement"')
        print(f"Total records: {cursor.fetchone()[0]}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Income statement table cleaned. Now run the pipeline once to get the correct single record.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    clean_income_statement()