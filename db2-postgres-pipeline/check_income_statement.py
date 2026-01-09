#!/usr/bin/env python3
"""
Check Income Statement Records
"""

import psycopg2
from config import Config

def check_income_statement():
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
        
        # Check count and dates
        cursor.execute('SELECT COUNT(*), MIN("reportingDate"), MAX("reportingDate") FROM "incomeStatement"')
        result = cursor.fetchone()
        print(f"Income statement records: {result[0]}")
        print(f"First record: {result[1]}")
        print(f"Last record: {result[2]}")
        
        # Show all records
        cursor.execute('SELECT "id", "reportingDate", "interestIncome", "interestExpense" FROM "incomeStatement" ORDER BY "reportingDate"')
        records = cursor.fetchall()
        
        print("\nAll income statement records:")
        for record in records:
            print(f"  ID: {record[0]}, Date: {record[1]}, Interest Income: {record[2]}, Interest Expense: {record[3]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_income_statement()