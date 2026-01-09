#!/usr/bin/env python3
"""
Fix Income Statement Duplicate Records
"""

import psycopg2
from config import Config

def fix_duplicates():
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
        
        print("Before cleanup:")
        cursor.execute('SELECT COUNT(*) FROM "incomeStatement"')
        print(f"Total records: {cursor.fetchone()[0]}")
        
        # Keep only the latest record (most recent reportingDate)
        print("\nRemoving duplicate records...")
        cursor.execute("""
            DELETE FROM "incomeStatement" 
            WHERE "id" NOT IN (
                SELECT "id" 
                FROM "incomeStatement" 
                ORDER BY "reportingDate" DESC 
                LIMIT 1
            )
        """)
        
        deleted_count = cursor.rowcount
        print(f"Deleted {deleted_count} duplicate records")
        
        # Add unique constraint on reportingDate (truncated to date only)
        print("\nAdding unique constraint...")
        try:
            cursor.execute('ALTER TABLE "incomeStatement" ADD CONSTRAINT unique_income_statement_date UNIQUE (DATE("reportingDate"))')
            print("✅ Unique constraint added successfully")
        except Exception as e:
            if "already exists" in str(e):
                print("✅ Unique constraint already exists")
            else:
                print(f"⚠️ Could not add unique constraint: {e}")
        
        conn.commit()
        
        print("\nAfter cleanup:")
        cursor.execute('SELECT COUNT(*) FROM "incomeStatement"')
        print(f"Total records: {cursor.fetchone()[0]}")
        
        # Show remaining record
        cursor.execute('SELECT "id", "reportingDate", "interestIncome", "interestExpense", "nonInterestIncome" FROM "incomeStatement"')
        record = cursor.fetchone()
        if record:
            print(f"\nRemaining record:")
            print(f"  ID: {record[0]}")
            print(f"  Date: {record[1]}")
            print(f"  Interest Income: {record[2]:,.2f}")
            print(f"  Interest Expense: {record[3]:,.2f}")
            print(f"  Non-Interest Income: {record[4]:,.2f}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix_duplicates()