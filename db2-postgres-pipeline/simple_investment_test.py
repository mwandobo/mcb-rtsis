#!/usr/bin/env python3
"""
Simple Investment Debt Securities test
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def simple_test():
    """Simple test for investment debt securities data"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test 1: Check GLG_ACCOUNT for any 130% pattern
            logger.info("🔍 Checking GLG_ACCOUNT for 130% pattern...")
            
            query1 = """
            SELECT ACCOUNT_ID, EXTERNAL_GLACCOUNT
            FROM GLG_ACCOUNT 
            WHERE EXTERNAL_GLACCOUNT LIKE '130%'
            FETCH FIRST 10 ROWS ONLY
            """
            
            cursor.execute(query1)
            results1 = cursor.fetchall()
            logger.info(f"Found {len(results1)} GL accounts with 130% pattern:")
            for row in results1:
                logger.info(f"   {row[0]} | {row[1]}")
            
            # Test 2: Check DEPOSIT_ACCOUNT counts by type
            logger.info("🔍 Checking DEPOSIT_ACCOUNT by type...")
            
            query2 = """
            SELECT DEPOSIT_TYPE, COUNT(*) as count
            FROM DEPOSIT_ACCOUNT 
            WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            GROUP BY DEPOSIT_TYPE
            ORDER BY DEPOSIT_TYPE
            """
            
            cursor.execute(query2)
            results2 = cursor.fetchall()
            logger.info(f"DEPOSIT_ACCOUNT by type:")
            for row in results2:
                logger.info(f"   Type {row[0]}: {row[1]} records")
            
            # Test 3: Check active DEPOSIT_ACCOUNT with balances
            logger.info("🔍 Checking active DEPOSIT_ACCOUNT with balances...")
            
            query3 = """
            SELECT DEPOSIT_TYPE, COUNT(*) as count
            FROM DEPOSIT_ACCOUNT 
            WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND ENTRY_STATUS IN ('1', '6')
            AND (BOOK_BALANCE > 0 OR AVAILABLE_BALANCE > 0 OR OPENING_BALANCE > 0)
            GROUP BY DEPOSIT_TYPE
            ORDER BY DEPOSIT_TYPE
            """
            
            cursor.execute(query3)
            results3 = cursor.fetchall()
            logger.info(f"Active DEPOSIT_ACCOUNT with balances:")
            for row in results3:
                logger.info(f"   Type {row[0]}: {row[1]} records")
            
            # Test 4: Sample DEPOSIT_ACCOUNT records
            if results3:
                logger.info("🔍 Sample DEPOSIT_ACCOUNT records...")
                
                query4 = """
                SELECT ACCOUNT_NUMBER, DEPOSIT_TYPE, BOOK_BALANCE, AVAILABLE_BALANCE, OPENING_BALANCE
                FROM DEPOSIT_ACCOUNT 
                WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
                AND ENTRY_STATUS IN ('1', '6')
                AND (BOOK_BALANCE > 0 OR AVAILABLE_BALANCE > 0 OR OPENING_BALANCE > 0)
                FETCH FIRST 5 ROWS ONLY
                """
                
                cursor.execute(query4)
                results4 = cursor.fetchall()
                for row in results4:
                    logger.info(f"   Account: {row[0]} | Type: {row[1]} | Book: {row[2]} | Available: {row[3]} | Opening: {row[4]}")
            
    except Exception as e:
        logger.error(f"❌ Error in simple test: {e}")
        raise

if __name__ == "__main__":
    simple_test()