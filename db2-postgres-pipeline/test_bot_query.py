#!/usr/bin/env python3
"""
Test BOT balances query
"""

from db2_connection import DB2Connection
from config import Config

def test_bot_query():
    """Test BOT balances query"""
    config = Config()
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test the BOT query
            bot_query = config.tables['balances_bot'].query
            print("🏛️ Testing BOT balances query...")
            print(f"Query: {bot_query[:100]}...")
            
            cursor.execute(bot_query)
            rows = cursor.fetchall()
            
            print(f"✅ BOT query returned {len(rows)} records")
            
            if rows:
                print("\n📋 Sample BOT balance data:")
                for i, row in enumerate(rows[:3], 1):
                    print(f"  {i}. Account: {row[1]}, Amount: {row[6]:,.2f} {row[5]}")
            else:
                print("⚠️ No BOT balance records found")
                
    except Exception as e:
        print(f"❌ BOT query error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bot_query()