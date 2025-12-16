#!/usr/bin/env python3
"""
Test BOT balances pipeline only
"""

import psycopg2
from db2_connection import DB2Connection
from config import Config
from processors.bot_balances_processor import BotBalancesProcessor, BotBalancesRecord

def test_bot_pipeline():
    """Test BOT balances pipeline"""
    config = Config()
    db2_conn = DB2Connection()
    bot_processor = BotBalancesProcessor()
    
    try:
        # Step 1: Fetch from DB2
        print("🏛️ Fetching BOT balances from DB2...")
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            bot_query = config.tables['balances_bot'].query
            cursor.execute(bot_query)
            rows = cursor.fetchall()
            
            print(f"✅ Fetched {len(rows)} BOT balance records")
            
            if not rows:
                print("⚠️ No BOT balance records found")
                return
            
            # Step 2: Process records
            print("🔄 Processing BOT balance records...")
            records = []
            for row in rows[:5]:  # Test with first 5 records
                try:
                    record = bot_processor.process_record(row, 'balances_bot')
                    if bot_processor.validate_record(record):
                        records.append(record)
                        print(f"  ✅ Processed: Account {record.account_number}, {record.org_amount:,.2f} {record.currency}")
                    else:
                        print(f"  ❌ Invalid record: {row}")
                except Exception as e:
                    print(f"  ❌ Processing error: {e}")
            
            # Step 3: Insert to PostgreSQL
            if records:
                print(f"\n💾 Inserting {len(records)} records to PostgreSQL...")
                conn_pg = psycopg2.connect(
                    host=config.database.pg_host,
                    port=config.database.pg_port,
                    database=config.database.pg_database,
                    user=config.database.pg_user,
                    password=config.database.pg_password
                )
                
                cursor_pg = conn_pg.cursor()
                
                for record in records:
                    try:
                        bot_processor.insert_to_postgres(record, cursor_pg)
                        conn_pg.commit()
                        print(f"  ✅ Inserted: Account {record.account_number}")
                    except Exception as e:
                        print(f"  ❌ Insert error: {e}")
                        conn_pg.rollback()
                
                cursor_pg.close()
                conn_pg.close()
                
                print(f"\n🎉 BOT balances pipeline test completed!")
            
    except Exception as e:
        print(f"❌ BOT pipeline test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bot_pipeline()