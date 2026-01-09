#!/usr/bin/env python3
"""
Check ICBM Transaction Status
"""

import psycopg2
from config import Config

def check_icbm_status():
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
        
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'icbmTransaction'
        """)
        table_exists = cursor.fetchone()[0] > 0
        print(f"ICBM table exists: {table_exists}")
        
        if table_exists:
            # Check record count
            cursor.execute('SELECT COUNT(*) FROM "icbmTransaction"')
            count = cursor.fetchone()[0]
            print(f"ICBM transaction records: {count}")
            
            if count > 0:
                # Show sample records
                cursor.execute("""
                    SELECT "transactionDate", "transactionType", "tzsAmount" 
                    FROM "icbmTransaction" 
                    ORDER BY "transactionDate" DESC 
                    LIMIT 5
                """)
                records = cursor.fetchall()
                print("\nSample ICBM transactions:")
                for record in records:
                    print(f"  {record[0]} | {record[1]} | {record[2]:,.2f} TZS")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_icbm_status()