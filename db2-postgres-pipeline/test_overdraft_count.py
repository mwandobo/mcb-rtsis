#!/usr/bin/env python3
"""
Test overdraft record counts with different date ranges
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_overdraft_counts():
    """Test overdraft counts with different filters"""
    logger.info("🧪 Testing Overdraft Record Counts")
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test 1: All overdraft records
            logger.info("Test 1: All overdraft records...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT 
                WHERE OVERDRAFT_TYPE_FLAG = 'Overdraft'
                FETCH FIRST 1 ROWS ONLY
            """)
            all_count = cursor.fetchone()[0]
            logger.info(f"✅ Total overdraft records: {all_count:,}")
            
            # Test 2: With amount filter
            logger.info("Test 2: With amount > 0 filter...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT 
                WHERE OVERDRAFT_TYPE_FLAG = 'Overdraft'
                  AND ACC_LIMIT_AMN > 0
                FETCH FIRST 1 ROWS ONLY
            """)
            amount_count = cursor.fetchone()[0]
            logger.info(f"✅ Overdraft records with amount > 0: {amount_count:,}")
            
            # Test 3: Recent records (30 days)
            logger.info("Test 3: Recent records (30 days)...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT 
                WHERE OVERDRAFT_TYPE_FLAG = 'Overdraft'
                  AND ACC_LIMIT_AMN > 0
                  AND EOM_DATE >= CURRENT DATE - 30 DAYS
                FETCH FIRST 1 ROWS ONLY
            """)
            recent_count = cursor.fetchone()[0]
            logger.info(f"✅ Recent overdraft records (30 days): {recent_count:,}")
            
            # Test 4: Very recent (7 days)
            logger.info("Test 4: Very recent records (7 days)...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT 
                WHERE OVERDRAFT_TYPE_FLAG = 'Overdraft'
                  AND ACC_LIMIT_AMN > 0
                  AND EOM_DATE >= CURRENT DATE - 7 DAYS
                FETCH FIRST 1 ROWS ONLY
            """)
            very_recent_count = cursor.fetchone()[0]
            logger.info(f"✅ Very recent overdraft records (7 days): {very_recent_count:,}")
            
            # If we have any records, get a sample
            if amount_count > 0:
                logger.info("Getting sample records...")
                cursor.execute("""
                    SELECT 
                        CUSTOMER_NAME,
                        CURRENCY,
                        ACC_LIMIT_AMN,
                        EOM_DATE
                    FROM W_EOM_LOAN_ACCOUNT 
                    WHERE OVERDRAFT_TYPE_FLAG = 'Overdraft'
                      AND ACC_LIMIT_AMN > 0
                    ORDER BY EOM_DATE DESC
                    FETCH FIRST 3 ROWS ONLY
                """)
                samples = cursor.fetchall()
                
                logger.info("📋 Sample overdraft records:")
                for i, row in enumerate(samples, 1):
                    logger.info(f"  {i}. {row[0]} - {row[1]} - {row[2]:,.2f} - {row[3]}")
            
    except Exception as e:
        logger.error(f"❌ Overdraft count test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_overdraft_counts()