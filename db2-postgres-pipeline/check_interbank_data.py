#!/usr/bin/env python3
"""
Quick check for interbank loan payable data
"""

import psycopg2
from config import Config

def check_data():
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
        cursor.execute("SELECT COUNT(*) FROM interbankLoanPayable")
        count = cursor.fetchone()[0]
        
        print(f"Current records in interbankLoanPayable table: {count:,}")
        
        if count > 0:
            cursor.execute("SELECT * FROM interbankLoanPayable LIMIT 5")
            rows = cursor.fetchall()
            print(f"Sample records:")
            for i, row in enumerate(rows, 1):
                print(f"  {i}. Account: {row[2]}, Amount: {row[9]}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking data: {e}")

if __name__ == "__main__":
    check_data()