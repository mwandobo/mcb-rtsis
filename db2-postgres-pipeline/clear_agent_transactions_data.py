#!/usr/bin/env python3
"""
Clear all data from agentTransactions table
"""

import psycopg2
from config import Config

def clear_agent_transactions_data():
    """Delete all data from agentTransactions table"""
    
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
        
        print("🗑️ CLEARING AGENT TRANSACTIONS DATA")
        print("=" * 50)
        
        # Check current count
        cursor.execute('SELECT COUNT(*) FROM "agentTransactions"')
        current_count = cursor.fetchone()[0]
        print(f"📊 Current records: {current_count:,}")
        
        if current_count > 0:
            # Delete all data
            cursor.execute('DELETE FROM "agentTransactions"')
            deleted_count = cursor.rowcount
            
            # Commit the changes
            conn.commit()
            
            print(f"🗑️ Deleted {deleted_count:,} records")
            
            # Verify deletion
            cursor.execute('SELECT COUNT(*) FROM "agentTransactions"')
            final_count = cursor.fetchone()[0]
            print(f"📊 Remaining records: {final_count:,}")
            
            if final_count == 0:
                print("✅ All data cleared successfully!")
            else:
                print(f"⚠️ Warning: {final_count} records still remain")
        else:
            print("ℹ️ Table is already empty")
        
        cursor.close()
        conn.close()
        
        print("=" * 50)
        print("✅ CLEAR OPERATION COMPLETED")
        
    except Exception as e:
        print(f"❌ Failed to clear data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clear_agent_transactions_data()