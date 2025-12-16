#!/usr/bin/env python3
"""
Check data in PostgreSQL tables
"""

import psycopg2
from config import Config

def check_data():
    """Check data in both tables"""
    config = Config()
    
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Check cash_information
    cursor.execute('SELECT COUNT(*) FROM cash_information')
    cash_count = cursor.fetchone()[0]
    print(f"💰 Cash records: {cash_count:,}")
    
    # Check asset_owned
    cursor.execute('SELECT COUNT(*) FROM asset_owned')
    assets_count = cursor.fetchone()[0]
    print(f"🏢 Asset records: {assets_count:,}")
    
    # Check balances_bot
    cursor.execute('SELECT COUNT(*) FROM balances_bot')
    bot_count = cursor.fetchone()[0]
    print(f"🏛️ BOT balance records: {bot_count:,}")
    
    # Check balances_with_mnos
    cursor.execute('SELECT COUNT(*) FROM balances_with_mnos')
    mnos_count = cursor.fetchone()[0]
    print(f"📱 MNOs balance records: {mnos_count:,}")
    
    # Check balance_with_other_bank
    cursor.execute('SELECT COUNT(*) FROM balance_with_other_bank')
    other_banks_count = cursor.fetchone()[0]
    print(f"🏦 Other Banks balance records: {other_banks_count:,}")
    
    # Check other_assets
    cursor.execute('SELECT COUNT(*) FROM other_assets')
    other_assets_count = cursor.fetchone()[0]
    print(f"💎 Other Assets records: {other_assets_count:,}")
    
    if assets_count > 0:
        print("\n📋 Sample asset data:")
        cursor.execute('SELECT "assetType", "assetCategory", "orgCostValue", currency FROM asset_owned LIMIT 5')
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"  {i}. {row[0]} - {row[1]} - {row[2]:,.2f} {row[3]}")
    
    if cash_count > 0:
        print("\n💰 Sample cash data:")
        cursor.execute('SELECT "branchCode", "cashCategory", "orgAmount", currency FROM cash_information LIMIT 5')
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"  {i}. Branch {row[0]} - {row[1]} - {row[2]:,.2f} {row[3]}")
    
    if bot_count > 0:
        print("\n🏛️ Sample BOT balance data:")
        cursor.execute('SELECT "accountNumber", "accountName", "orgAmount", currency FROM balances_bot LIMIT 5')
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"  {i}. Account {row[0]} - {row[1]} - {row[2]:,.2f} {row[3]}")
    
    if mnos_count > 0:
        print("\n📱 Sample MNOs balance data:")
        cursor.execute('SELECT "mnoCode", "tillNumber", "orgFloatAmount", currency FROM balances_with_mnos LIMIT 5')
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"  {i}. {row[0]} - Till {row[1]} - {row[2]:,.2f} {row[3]}")
    
    if other_banks_count > 0:
        print("\n🏦 Sample Other Banks balance data:")
        cursor.execute('SELECT "accountName", "bankCode", "orgAmount", currency FROM balance_with_other_bank LIMIT 5')
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"  {i}. {row[0]} - {row[1]} - {row[2]:,.2f} {row[3]}")
    
    if other_assets_count > 0:
        print("\n💎 Sample Other Assets data:")
        cursor.execute('SELECT "assetType", "debtorName", "orgAmount", currency FROM other_assets LIMIT 5')
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"  {i}. {row[0]} - {row[1]} - {row[2]:,.2f} {row[3]}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_data()