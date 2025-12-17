#!/usr/bin/env python3
"""
Test Basic DB2 Query Access
"""

import logging
from db2_connection import DB2Connection

def test_basic_queries():
    """Test basic DB2 queries to verify access"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 Testing Basic DB2 Query Access")
    logger.info("=" * 50)
    
    try:
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test 1: Simple system query
            logger.info("🔌 Test 1: System query...")
            cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()
            logger.info(f"✅ System time: {result[0]}")
            
            # Test 2: Check if GLI_TRX_EXTRACT table exists
            logger.info("\n📋 Test 2: Check table existence...")
            try:
                cursor.execute("SELECT COUNT(*) FROM GLI_TRX_EXTRACT FETCH FIRST 1 ROWS ONLY")
                logger.info("✅ GLI_TRX_EXTRACT table accessible")
            except Exception as e:
                logger.error(f"❌ GLI_TRX_EXTRACT access error: {e}")
                return False
            
            # Test 3: Check GLG_ACCOUNT table
            logger.info("\n📋 Test 3: Check GLG_ACCOUNT table...")
            try:
                cursor.execute("SELECT COUNT(*) FROM GLG_ACCOUNT FETCH FIRST 1 ROWS ONLY")
                logger.info("✅ GLG_ACCOUNT table accessible")
            except Exception as e:
                logger.error(f"❌ GLG_ACCOUNT access error: {e}")
                return False
            
            # Test 4: Simple join query (limited)
            logger.info("\n🔗 Test 4: Simple join query...")
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM GLI_TRX_EXTRACT gte 
                    JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                    FETCH FIRST 1 ROWS ONLY
                """)
                logger.info("✅ Join query works")
            except Exception as e:
                logger.error(f"❌ Join query error: {e}")
                return False
            
            # Test 5: Check for any cash-related GL accounts
            logger.info("\n💰 Test 5: Check cash GL accounts...")
            try:
                cursor.execute("""
                    SELECT gl.EXTERNAL_GLACCOUNT, COUNT(*) as cnt
                    FROM GLI_TRX_EXTRACT gte 
                    JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                    WHERE gl.EXTERNAL_GLACCOUNT LIKE '1010%'
                    GROUP BY gl.EXTERNAL_GLACCOUNT
                    FETCH FIRST 5 ROWS ONLY
                """)
                results = cursor.fetchall()
                
                if results:
                    logger.info("✅ Found cash-related GL accounts:")
                    for account, count in results:
                        logger.info(f"   {account}: {count:,} records")
                else:
                    logger.info("⚠️ No cash-related GL accounts found")
                    
            except Exception as e:
                logger.error(f"❌ Cash GL accounts check error: {e}")
            
            # Test 6: Check recent data (simple)
            logger.info("\n📅 Test 6: Check for any recent data...")
            try:
                cursor.execute("""
                    SELECT MAX(TRN_DATE) as latest_date
                    FROM GLI_TRX_EXTRACT
                """)
                result = cursor.fetchone()
                if result and result[0]:
                    logger.info(f"✅ Latest transaction date: {result[0]}")
                else:
                    logger.info("⚠️ No transaction dates found")
                    
            except Exception as e:
                logger.error(f"❌ Recent data check error: {e}")
            
            logger.info("\n✅ Basic query tests completed!")
            return True
            
    except Exception as e:
        logger.error(f"❌ Basic query test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 Testing Basic DB2 Query Access")
    print("=" * 40)
    
    success = test_basic_queries()
    
    if success:
        print("\n✅ All basic tests passed!")
        print("💡 The pipeline infrastructure is working correctly")
        print("💡 If no cash records are found, it might be normal for a test database")
    else:
        print("\n❌ Some tests failed - check the logs above")