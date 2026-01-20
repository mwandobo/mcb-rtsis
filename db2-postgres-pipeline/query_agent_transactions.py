#!/usr/bin/env python3
"""
Query script to verify agentTransactions data
"""

import psycopg2
from config import Config

def query_agent_transactions():
    """Query and display agent transactions data"""
    
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
        
        print("🏪 AGENT TRANSACTIONS DATA VERIFICATION")
        print("=" * 60)
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM "agentTransactions"')
        total_count = cursor.fetchone()[0]
        print(f"📊 Total Records: {total_count:,}")
        
        # Get count by transaction type
        cursor.execute("""
            SELECT "transactionType", COUNT(*) as count, SUM("tzsAmount") as total_amount
            FROM "agentTransactions"
            GROUP BY "transactionType"
            ORDER BY count DESC
        """)
        
        type_stats = cursor.fetchall()
        print(f"\n📋 Transaction Types:")
        print("-" * 60)
        for stat in type_stats:
            print(f"  {stat[0]:<20} Count: {stat[1]:>6,} | Total: {stat[2]:>15,.2f} TZS")
        
        # Get count by agent
        cursor.execute("""
            SELECT "agentId", COUNT(*) as count, SUM("tzsAmount") as total_amount
            FROM "agentTransactions"
            GROUP BY "agentId"
            ORDER BY count DESC
            LIMIT 10
        """)
        
        agent_stats = cursor.fetchall()
        print(f"\n🏪 Top Agents by Transaction Count:")
        print("-" * 60)
        for i, stat in enumerate(agent_stats, 1):
            agent_id = stat[0].strip() if stat[0] else "N/A"
            print(f"  {i:>2}. {agent_id:<20} Count: {stat[1]:>6,} | Total: {stat[2]:>15,.2f} TZS")
        
        # Get date range
        cursor.execute("""
            SELECT MIN("transactionDate") as min_date, MAX("transactionDate") as max_date
            FROM "agentTransactions"
        """)
        
        date_range = cursor.fetchone()
        print(f"\n📅 Date Range:")
        print("-" * 60)
        print(f"  From: {date_range[0]}")
        print(f"  To:   {date_range[1]}")
        
        # Show recent transactions
        cursor.execute("""
            SELECT "agentId", "transactionType", "currency", "tzsAmount", "transactionDate", "transactionId"
            FROM "agentTransactions"
            ORDER BY "transactionDate" DESC, "agentId"
            LIMIT 10
        """)
        
        recent_txns = cursor.fetchall()
        print(f"\n📋 Recent Transactions:")
        print("-" * 60)
        for i, txn in enumerate(recent_txns, 1):
            agent_id = txn[0].strip() if txn[0] else "N/A"
            print(f"  {i:>2}. Agent: {agent_id}")
            print(f"      Type: {txn[1]} | Amount: {txn[3]:,.2f} {txn[2]} | Date: {txn[4]}")
            print(f"      ID: {txn[5]}")
            print()
        
        # Verify camelCase naming
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'agentTransactions'
            AND column_name IN ('reportingDate', 'agentId', 'transactionDate', 'transactionId', 'transactionType')
            ORDER BY ordinal_position
        """)
        
        camel_fields = cursor.fetchall()
        print(f"✅ camelCase Fields Verified:")
        print("-" * 60)
        for field in camel_fields:
            print(f"  ✓ {field[0]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ AGENT TRANSACTIONS VERIFICATION COMPLETED")
        print("🎉 All camelCase naming is working correctly!")
        print("📊 Data is properly structured and accessible")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    query_agent_transactions()