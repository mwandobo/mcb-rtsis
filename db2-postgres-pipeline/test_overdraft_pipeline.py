#!/usr/bin/env python3
"""
Test Overdraft Pipeline Component
"""

import logging
from db2_connection import DB2Connection
from config import Config
from processors.overdraft_processor import OverdraftProcessor
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_overdraft_fetch():
    """Test fetching overdraft data from DB2"""
    logger.info("🧪 Testing Overdraft Data Fetch from DB2")
    
    config = Config()
    db2_conn = DB2Connection()
    overdraft_config = config.tables['overdraft']
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Use limited query for testing
            test_query = overdraft_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 5 ROWS ONLY")
            
            logger.info("📊 Executing overdraft query...")
            cursor.execute(test_query)
            rows = cursor.fetchall()
            
            logger.info(f"✅ Fetched {len(rows)} overdraft records")
            
            if rows:
                logger.info("📋 Sample overdraft data:")
                for i, row in enumerate(rows[:3], 1):
                    logger.info(f"  {i}. Account: {row[1]}, Client: {row[3]}, Amount: {row[23]:,.2f} {row[22]}")
            
            return rows
            
    except Exception as e:
        logger.error(f"❌ Overdraft fetch test failed: {e}")
        import traceback
        traceback.print_exc()
        return []

def test_overdraft_processing():
    """Test overdraft data processing"""
    logger.info("🧪 Testing Overdraft Data Processing")
    
    # First fetch some data
    rows = test_overdraft_fetch()
    
    if not rows:
        logger.warning("⚠️ No overdraft data to process")
        return []
    
    processor = OverdraftProcessor()
    processed_records = []
    
    try:
        for row in rows:
            record = processor.process_record(row, 'overdraft')
            
            if processor.validate_record(record):
                processed_records.append(record)
                logger.info(f"✅ Processed: Account {record.account_number}, Client: {record.client_name}")
            else:
                logger.warning(f"⚠️ Invalid record: Account {record.account_number}")
        
        logger.info(f"✅ Successfully processed {len(processed_records)} overdraft records")
        return processed_records
        
    except Exception as e:
        logger.error(f"❌ Overdraft processing test failed: {e}")
        import traceback
        traceback.print_exc()
        return []

def test_overdraft_insertion():
    """Test inserting overdraft data to PostgreSQL"""
    logger.info("🧪 Testing Overdraft Data Insertion to PostgreSQL")
    
    # First process some data
    records = test_overdraft_processing()
    
    if not records:
        logger.warning("⚠️ No processed overdraft records to insert")
        return
    
    config = Config()
    processor = OverdraftProcessor()
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        cursor = conn.cursor()
        
        # Clear existing test data
        cursor.execute("DELETE FROM overdraft WHERE \"accountNumber\" LIKE 'TEST%'")
        
        inserted_count = 0
        for record in records:
            try:
                processor.insert_to_postgres(record, cursor)
                inserted_count += 1
                logger.info(f"✅ Inserted: Account {record.account_number}")
            except Exception as e:
                logger.error(f"❌ Failed to insert Account {record.account_number}: {e}")
        
        conn.commit()
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM overdraft")
        total_count = cursor.fetchone()[0]
        
        logger.info(f"✅ Successfully inserted {inserted_count} overdraft records")
        logger.info(f"📊 Total overdraft records in database: {total_count}")
        
        # Show sample data
        cursor.execute('SELECT "accountNumber", "clientName", "orgSanctionedAmount", currency FROM overdraft LIMIT 5')
        sample_rows = cursor.fetchall()
        
        if sample_rows:
            logger.info("📋 Sample overdraft data in PostgreSQL:")
            for i, row in enumerate(sample_rows, 1):
                logger.info(f"  {i}. Account {row[0]} - {row[1]} - {row[2]:,.2f} {row[3]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Overdraft insertion test failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all overdraft tests"""
    logger.info("🚀 Starting Overdraft Pipeline Component Tests")
    logger.info("=" * 50)
    
    try:
        # Test 1: Data fetch
        test_overdraft_fetch()
        
        # Test 2: Data processing  
        test_overdraft_processing()
        
        # Test 3: Data insertion
        test_overdraft_insertion()
        
        logger.info("🎉 All overdraft tests completed!")
        
    except Exception as e:
        logger.error(f"❌ Overdraft tests failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()