#!/usr/bin/env python3
"""
Query script to verify mobileBanking data
"""

import psycopg2
from config import Config

def query_mobile_banking():
    """Query and display mobile banking data"""
    
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
        
        print("🏦 MOBILE BANKING DATA VERIFICATION")
        print("=" * 60)
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM "mobileBanking"')
        total_count = cursor.fetchone()[0]
        print(f"📊 Total Mobile Banking Records: {total_count:,}")
        
        # Get count by transaction type
        cursor.execute("""
            SELECT "mobileTransactionType", COUNT(*) as count
            FROM "mobileBanking"
            GROUP BY "mobileTransactionType"
            ORDER BY count DESC
        """)
        
        type_stats = cursor.fetchall()
        print(f"\n📋 Mobile Banking by Transaction Type:")
        print("-" * 60)
        for stat in type_stats:
            print(f"  {stat[0]:<15} Count: {stat[1]:>6,}")
        
        # Get count by currency
        cursor.execute("""
            SELECT "currency", COUNT(*) as count, 
                   SUM("orgAmount") as total_amount
            FROM "mobileBanking"
            GROUP BY "currency"
            ORDER BY count DESC
        """)
        
        currency_stats = cursor.fetchall()
        print(f"\n💰 Mobile Banking by Currency:")
        print("-" * 60)
        for stat in currency_stats:
            currency = stat[0].strip() if stat[0] else "N/A"
            count = stat[1]
            total_amount = float(stat[2]) if stat[2] else 0
            print(f"  {currency:<10} Count: {count:>6,} Total: {total_amount:>15,.2f}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ MOBILE BANKING VERIFICATION COMPLETED")
        print("🎉 All camelCase naming is working correctly!")
        print("📊 Data is properly structured and accessible")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Query failed: {e}")

if __name__ == "__main__":
    query_mobile_banking()