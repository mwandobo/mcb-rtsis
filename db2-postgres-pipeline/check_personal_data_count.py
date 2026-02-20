#!/usr/bin/env python3
"""
Check count of records in personalData table
"""

import psycopg2
from config import Config

def check_count():
    """Check record count"""
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
        
        cursor.execute('SELECT COUNT(*) FROM "personalData"')
        count = cursor.fetchone()[0]
        
        print(f"📊 personalData table has {count:,} records")
        
        if count > 0:
            cursor.execute('SELECT "customerIdentificationNumber", "firstName", "otherNames", "monthlyIncome", "region", "district" FROM "personalData" LIMIT 5')
            records = cursor.fetchall()
            print("\n📋 Sample records:")
            for i, rec in enumerate(records, 1):
                print(f"  {i}. Customer: {rec[0]}, Name: {rec[1]} {rec[2]}, Income: {rec[3]}, Location: {rec[4]}/{rec[5]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_count()
