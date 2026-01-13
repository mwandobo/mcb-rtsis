#!/usr/bin/env python3
"""
Verify Balance with Other Banks data
"""

import psycopg2
from config import Config

def verify_balance_other_banks():
    """Verify the balance with other banks data"""
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
        
        print("🏦 BALANCE WITH OTHER BANKS VERIFICATION")
        print("=" * 50)
        
        # Total records
        cursor.execute('SELECT COUNT(*) FROM "balanceWithOtherBank"')
        total_count = cursor.fetchone()[0]
        print(f"📊 Total records: {total_count}")
        
        # Records by bank code
        cursor.execute('SELECT "bankCode", COUNT(*) FROM "balanceWithOtherBank" GROUP BY "bankCode" ORDER BY COUNT(*) DESC')
        print("\n📋 Records by bank code:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]} records")
        
        # Records by currency
        cursor.execute('SELECT "currency", COUNT(*) FROM "balanceWithOtherBank" GROUP BY "currency" ORDER BY COUNT(*) DESC')
        print("\n💰 Records by currency:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]} records")
        
        # Sample records with enhanced bank mapping
        cursor.execute('''
            SELECT "accountName", "accountNumber", "bankCode", "currency", "orgAmount", "usdAmount", "tzsAmount"
            FROM "balanceWithOtherBank" 
            ORDER BY "transactionDate" DESC 
            LIMIT 10
        ''')
        
        print("\n📋 Sample records with updated bank codes:")
        for row in cursor.fetchall():
            account_name, account_number, bank_code, currency, org_amount, usd_amount, tzs_amount = row
            print(f"  - {account_name[:30]:<30} ({account_number}) Bank:{bank_code} {currency} {org_amount}")
            if usd_amount:
                print(f"    USD: {usd_amount}, TZS: {tzs_amount}")
        
        # Amount statistics
        cursor.execute('''
            SELECT 
                COUNT(*) as total_records,
                SUM("orgAmount") as total_org_amount,
                SUM("usdAmount") as total_usd_amount,
                SUM("tzsAmount") as total_tzs_amount,
                AVG("orgAmount") as avg_org_amount
            FROM "balanceWithOtherBank"
        ''')
        
        stats = cursor.fetchone()
        print(f"\n💵 Amount Statistics:")
        print(f"  - Total Original Amount: {stats[1]:,.2f}")
        print(f"  - Total USD Amount: {stats[2]:,.2f}" if stats[2] else "  - Total USD Amount: 0.00")
        print(f"  - Total TZS Amount: {stats[3]:,.2f}")
        print(f"  - Average Original Amount: {stats[4]:,.2f}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Verification completed!")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")

if __name__ == "__main__":
    verify_balance_other_banks()