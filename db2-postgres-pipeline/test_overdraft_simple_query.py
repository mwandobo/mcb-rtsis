#!/usr/bin/env python3
"""
Test simplified overdraft query
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_overdraft_query():
    """Test overdraft query step by step"""
    logger.info("🧪 Testing Overdraft Query Step by Step")
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Step 1: Check basic table access
            logger.info("Step 1: Basic table access...")
            cursor.execute("SELECT COUNT(*) FROM W_EOM_LOAN_ACCOUNT FETCH FIRST 1 ROWS ONLY")
            count = cursor.fetchone()[0]
            logger.info(f"✅ W_EOM_LOAN_ACCOUNT accessible, sample count: {count}")
            
            # Step 2: Check overdraft flag
            logger.info("Step 2: Checking overdraft flag...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT 
                WHERE OVERDRAFT_TYPE_FLAG = 'Overdraft'
                FETCH FIRST 1 ROWS ONLY
            """)
            overdraft_count = cursor.fetchone()[0]
            logger.info(f"✅ Found {overdraft_count} overdraft records")
            
            # Step 3: Check date filter
            logger.info("Step 3: Checking date filter...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT 
                WHERE EOM_DATE >= CURRENT DATE - 300 DAYS
                FETCH FIRST 1 ROWS ONLY
            """)
            date_count = cursor.fetchone()[0]
            logger.info(f"✅ Found {date_count} records in date range")
            
            # Step 4: Combined filter
            logger.info("Step 4: Combined filters...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT 
                WHERE EOM_DATE >= CURRENT DATE - 300 DAYS
                  AND OVERDRAFT_TYPE_FLAG = 'Overdraft'
                FETCH FIRST 1 ROWS ONLY
            """)
            combined_count = cursor.fetchone()[0]
            logger.info(f"✅ Found {combined_count} overdraft records in date range")
            
            if combined_count > 0:
                # Step 5: Get basic fields
                logger.info("Step 5: Getting basic fields...")
                cursor.execute("""
                    SELECT 
                        CUSTOMER_NAME,
                        CURRENCY,
                        ACC_LIMIT_AMN,
                        ACC_OPEN_DT
                    FROM W_EOM_LOAN_ACCOUNT 
                    WHERE EOM_DATE >= CURRENT DATE - 300 DAYS
                      AND OVERDRAFT_TYPE_FLAG = 'Overdraft'
                    FETCH FIRST 3 ROWS ONLY
                """)
                rows = cursor.fetchall()
                
                logger.info(f"✅ Retrieved {len(rows)} sample records:")
                for i, row in enumerate(rows, 1):
                    logger.info(f"  {i}. {row[0]} - {row[1]} - {row[2]:,.2f} - {row[3]}")
            else:
                logger.warning("⚠️ No overdraft records found with current filters")
            
    except Exception as e:
        logger.error(f"❌ Overdraft query test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_overdraft_query()