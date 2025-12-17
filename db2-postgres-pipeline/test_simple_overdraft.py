#!/usr/bin/env python3
"""
Simple test for overdraft query
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_simple_overdraft():
    """Test simple overdraft query"""
    logger.info("🧪 Testing Simple Overdraft Query")
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Simple test query first
            simple_query = """
            SELECT COUNT(*) 
            FROM W_EOM_LOAN_ACCOUNT wela
            WHERE wela.EOM_DATE >= CURRENT DATE - 300 DAYS
              and wela.OVERDRAFT_TYPE_FLAG = 'Overdraft'
            """
            
            logger.info("📊 Executing simple count query...")
            cursor.execute(simple_query)
            count = cursor.fetchone()[0]
            
            logger.info(f"✅ Found {count} overdraft records")
            
            if count > 0:
                # Test basic fields query
                basic_query = """
                SELECT wela.CUSTOMER_NAME, wela.CURRENCY, wela.ACC_LIMIT_AMN
                FROM W_EOM_LOAN_ACCOUNT wela
                WHERE wela.EOM_DATE >= CURRENT DATE - 300 DAYS
                  and wela.OVERDRAFT_TYPE_FLAG = 'Overdraft'
                FETCH FIRST 3 ROWS ONLY
                """
                
                logger.info("📊 Executing basic fields query...")
                cursor.execute(basic_query)
                rows = cursor.fetchall()
                
                logger.info(f"✅ Fetched {len(rows)} basic records")
                for i, row in enumerate(rows, 1):
                    logger.info(f"  {i}. Client: {row[0]}, Currency: {row[1]}, Amount: {row[2]:,.2f}")
            
    except Exception as e:
        logger.error(f"❌ Simple overdraft test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_overdraft()