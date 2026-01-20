#!/usr/bin/env python3
"""
Quick check of mobile banking data
"""

import psycopg2
from config import Config

def quick_check():
    """Quick check of mobile banking data"""
    
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
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM "mobileBanking"')
        total_count = cursor.fetchone()[0]
        print(f"Total Mobile Banking Records: {total_count}")
        
        # Get sample records
        cursor.execute('SELECT "transactionRef", "mobileTransactionType", "currency", "orgAmount" FROM "mobileBanking" LIMIT 10')
        records = cursor.fetchall()
        
        print(f"\nSample records:")
        for i, record in enumerate(records, 1):
            print(f"{i}. Ref: {record[0]}, Type: {record[1]}, Currency: {record[2]}, Amount: {record[3]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    quick_check()