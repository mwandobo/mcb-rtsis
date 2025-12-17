#!/usr/bin/env python3
"""
Test simplified overdraft query
"""

import logging
from db2_connection import DB2Connection
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_simplified_overdraft():
    """Test simplified overdraft query"""
    logger.info("🧪 Testing Simplified Overdraft Query")
    
    config = Config()
    db2_conn = DB2Connection()
    overdraft_config = config.tables['overdraft']
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test the simplified query with even fewer rows
            test_query = overdraft_config.query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 3 ROWS ONLY")
            
            logger.info("📊 Executing simplified overdraft query...")
            cursor.execute(test_query)
            rows = cursor.fetchall()
            
            logger.info(f"✅ Fetched {len(rows)} overdraft records")
            
            if rows:
                logger.info("📋 Sample overdraft data:")
                for i, row in enumerate(rows, 1):
                    logger.info(f"  {i}. Account: {row[1]}, Client: {row[3]}, Amount: {row[23]:,.2f} {row[22]}")
                    
                logger.info(f"📊 Total columns in result: {len(rows[0])}")
            else:
                logger.warning("⚠️ No overdraft records found")
            
            return rows
            
    except Exception as e:
        logger.error(f"❌ Simplified overdraft test failed: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    test_simplified_overdraft()