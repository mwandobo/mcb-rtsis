#!/usr/bin/env python3
"""Check cardInformation table status"""

import psycopg2
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

config = Config()
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
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'cardInformation'
    )
""")
exists = cursor.fetchone()[0]
print(f"Table exists: {exists}")

if exists:
    # Count records
    cursor.execute('SELECT COUNT(*) FROM "cardInformation"')
    count = cursor.fetchone()[0]
    print(f"Current records: {count:,}")
    
    # Check for duplicates
    cursor.execute('''
        SELECT "cardNumber", COUNT(*) as cnt 
        FROM "cardInformation" 
        GROUP BY "cardNumber" 
        HAVING COUNT(*) > 1 
        LIMIT 5
    ''')
    dupes = cursor.fetchall()
    print(f"Duplicate card numbers found: {len(dupes)}")
    if dupes:
        print("Sample duplicates:")
        for card_num, cnt in dupes:
            print(f"  {card_num}: {cnt} times")

conn.close()
