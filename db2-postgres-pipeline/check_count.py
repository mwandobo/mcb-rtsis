#!/usr/bin/env python3
import psycopg2
from config import Config

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
    print(f'Total records in loanInformation table: {count:,}')
    
    # Also check a sample record to verify data structure
    cursor.execute('SELECT "loanNumber", "clientName", "currency", "collateralPledged" FROM "loanInformation" LIMIT 3')
    records = cursor.fetchall()
    
    print("\nSample records:")
    for i, record in enumerate(records, 1):
        print(f"Record {i}: loanNumber={record[0]}, clientName={record[1]}, currency={record[2]}, collateralPledged={record[3]}")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")