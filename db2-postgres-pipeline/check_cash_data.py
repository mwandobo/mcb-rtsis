#!/usr/bin/env python3
"""
Quick check of cash data in PostgreSQL
"""

import psycopg2

def check_cash_data():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='bankdb',
            user='postgres',
            password='postgres'
        )
        cur = conn.cursor()
        
        # Check total count
        cur.execute('SELECT COUNT(*) FROM "cashInformation"')
        total_count = cur.fetchone()[0]
        print(f"✅ Total records in cashInformation: {total_count}")
        
        # Check sample records
        cur.execute('SELECT "reportingDate", "branchCode", "cashCategory", "currency", "orgAmount" FROM "cashInformation" LIMIT 5')
        records = cur.fetchall()
        
        print("\n📋 Sample records:")
        for i, record in enumerate(records, 1):
            print(f"  {i}. Date: {record[0]} | Branch: {record[1]} | Category: {record[2]} | {record[4]:,.2f} {record[3]}")
        
        conn.close()
        print(f"\n🎉 Cash data verification completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_cash_data()