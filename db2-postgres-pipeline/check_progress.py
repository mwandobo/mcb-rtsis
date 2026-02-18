#!/usr/bin/env python3
"""
Quick script to check pipeline progress
"""

import psycopg2
from config import Config

def check_progress():
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
        cursor.execute('SELECT COUNT(*) FROM "loanInformation"')
        count = cursor.fetchone()[0]
        print(f"Records in loanInformation table: {count:,}")
        
        # Get sample records
        cursor.execute('SELECT "loanNumber", "clientName", "currency" FROM "loanInformation" LIMIT 5')
        records = cursor.fetchall()
        
        if records:
            print("\nSample records:")
            for i, (loan_num, client, currency) in enumerate(records, 1):
                print(f"{i}. Loan: {loan_num}, Client: {client}, Currency: {currency}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_progress()