#!/usr/bin/env python3
"""
Check other_assets table details to understand the primary key conflicts
"""

import psycopg2
from config import Config

def check_other_assets_details():
    """Check detailed other_assets data"""
    config = Config()
    
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Check all records with their primary key components
    cursor.execute('''
        SELECT "assetType", "transactionDate", "orgAmount", "debtorName"
        FROM other_assets 
        ORDER BY "transactionDate", "orgAmount"
    ''')
    
    rows = cursor.fetchall()
    
    print("=== OTHER ASSETS TABLE DETAILS ===")
    print(f"Total records: {len(rows)}")
    print()
    print("Asset Type      | Transaction Date | Amount           | Debtor Name")
    print("-" * 80)
    
    for row in rows:
        asset_type = row[0]
        trans_date = row[1]
        amount = row[2]
        debtor_name = row[3] if row[3] else "N/A"
        
        print(f"{asset_type:<15} | {trans_date} | {amount:>15,.2f} | {debtor_name}")
    
    print()
    
    # Check for duplicate primary keys (this should show why only 3 records exist)
    cursor.execute('''
        SELECT "assetType", "transactionDate", COUNT(*) as duplicate_count
        FROM other_assets 
        GROUP BY "assetType", "transactionDate"
        HAVING COUNT(*) > 1
    ''')
    
    duplicates = cursor.fetchall()
    
    if duplicates:
        print("=== DUPLICATE PRIMARY KEYS FOUND ===")
        for dup in duplicates:
            print(f"Asset Type: {dup[0]}, Transaction Date: {dup[1]}, Count: {dup[2]}")
    else:
        print("=== NO DUPLICATE PRIMARY KEYS ===")
        print("This means the 10 insert attempts had duplicate (assetType, transactionDate) combinations")
        print("PostgreSQL's ON CONFLICT DO UPDATE overwrote earlier records with the same primary key")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_other_assets_details()