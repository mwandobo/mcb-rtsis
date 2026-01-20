#!/usr/bin/env python3
"""
Check which record is missing and try to insert it manually
"""

import psycopg2
from config import Config

def check_missing_record():
    """Check which of the 10 records is missing"""
    
    config = Config()
    
    # Expected transaction IDs from debug output
    expected_ids = [
        '100-99973051-2-2024-01-02-1861',
        '100-99973051-2-2024-01-02-1908', 
        '100-99973051-2-2024-01-02-1987',
        '100-99973051-2-2024-01-02-2008',
        '100-99973051-2-2024-01-02-2037',
        '100-99973051-2-2024-01-02-1877',
        '100-99973051-2-2024-01-02-1939',
        '100-99973051-2-2024-01-02-1984',
        '100-99973051-2-2024-01-02-2063',
        '100-99973051-2-2024-01-02-2064'  # This might be the missing one
    ]
    
    print("🔍 CHECKING MISSING RECORD")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Get all transaction IDs in database
        cursor.execute('SELECT "transactionId" FROM "agentTransactions" ORDER BY "transactionId"')
        existing_ids = [row[0] for row in cursor.fetchall()]
        
        print(f"📊 Expected records: {len(expected_ids)}")
        print(f"📊 Existing records: {len(existing_ids)}")
        
        # Find missing IDs
        missing_ids = []
        for expected_id in expected_ids:
            if expected_id not in existing_ids:
                missing_ids.append(expected_id)
        
        print(f"\n❌ Missing transaction IDs:")
        for missing_id in missing_ids:
            print(f"   - {missing_id}")
        
        # Find extra IDs (shouldn't happen)
        extra_ids = []
        for existing_id in existing_ids:
            if existing_id not in expected_ids:
                extra_ids.append(existing_id)
        
        if extra_ids:
            print(f"\n➕ Extra transaction IDs (unexpected):")
            for extra_id in extra_ids:
                print(f"   - {extra_id}")
        
        # Show existing records
        print(f"\n✅ Existing transaction IDs:")
        for existing_id in existing_ids:
            print(f"   - {existing_id}")
        
        # Try to manually insert the missing record (2064)
        if '100-99973051-2-2024-01-02-2064' in missing_ids:
            print(f"\n🔧 Attempting to manually insert missing record 2064...")
            
            try:
                insert_query = """
                INSERT INTO "agentTransactions" (
                    "reportingDate", "agentId", "agentStatus", "transactionDate", "transactionId",
                    "transactionType", "serviceChannel", "tillNumber", "currency", "tzsAmount"
                ) VALUES (
                    CURRENT_TIMESTAMP, '60512579', 'active', '2024-01-02'::date, 
                    '100-99973051-2-2024-01-02-2064', 'Cash Withdraw', 'Point of Sale', 
                    NULL, 'TZS', 2000000.00
                )
                """
                
                cursor.execute(insert_query)
                conn.commit()
                
                print(f"   ✅ Successfully inserted missing record!")
                
                # Verify final count
                cursor.execute('SELECT COUNT(*) FROM "agentTransactions"')
                final_count = cursor.fetchone()[0]
                print(f"   📊 Final count: {final_count}")
                
            except Exception as e:
                print(f"   ❌ Failed to insert: {e}")
                conn.rollback()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_missing_record()