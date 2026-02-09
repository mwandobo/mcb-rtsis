#!/usr/bin/env python3
"""
Remove primary key constraint from loan information table to allow duplicates
"""

import psycopg2
from config import Config

def remove_primary_key():
    """Remove primary key constraint from loan information table"""
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
        
        print("Removing PRIMARY KEY constraint from loanInformation table...")
        
        # Drop the primary key constraint
        cursor.execute('ALTER TABLE "loanInformation" DROP CONSTRAINT "loanInformation_pkey"')
        conn.commit()
        
        print("✅ PRIMARY KEY constraint removed successfully!")
        print("✅ Table can now accept duplicate accountNumber values")
        
        # Check current row count
        cursor.execute('SELECT COUNT(*) FROM "loanInformation"')
        row_count = cursor.fetchone()[0]
        print(f"Current rows in table: {row_count}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    remove_primary_key()