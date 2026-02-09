#!/usr/bin/env python3
import psycopg2
from config import Config
import time

def check_count():
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
        
        # Check total count
        cursor.execute('SELECT COUNT(*) FROM "loanInformation"')
        total_count = cursor.fetchone()[0]
        print(f"Total records in database: {total_count:,}")
        
        # Check unique loan numbers
        cursor.execute('SELECT COUNT(DISTINCT "loanNumber") FROM "loanInformation"')
        unique_loans = cursor.fetchone()[0]
        print(f"Unique loan numbers: {unique_loans:,}")
        
        # Check for duplicates
        cursor.execute('''
            SELECT "loanNumber", COUNT(*) as count 
            FROM "loanInformation" 
            GROUP BY "loanNumber" 
            HAVING COUNT(*) > 1 
            ORDER BY count DESC 
            LIMIT 5
        ''')
        duplicates = cursor.fetchall()
        
        if duplicates:
            print(f"\nTop duplicate loan numbers:")
            for loan_num, count in duplicates:
                print(f"  {loan_num}: {count} records")
        else:
            print("\nNo duplicate loan numbers found")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_count()