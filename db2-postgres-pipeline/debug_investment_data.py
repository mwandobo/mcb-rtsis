#!/usr/bin/env python3
"""
Debug Investment Debt Securities data issues
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_investment_data():
    """Debug investment debt securities data issues"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test 1: Check ACCOUNT_NUMBER field in DEPOSIT_ACCOUNT
            logger.info("🔍 Checking ACCOUNT_NUMBER field in DEPOSIT_ACCOUNT...")
            
            query1 = """
            SELECT 
                ACCOUNT_NUMBER,
                DEPOSIT_TYPE,
                ENTRY_STATUS,
                BOOK_BALANCE,
                AVAILABLE_BALANCE,
                OPENING_BALANCE
            FROM DEPOSIT_ACCOUNT 
            WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND ENTRY_STATUS IN ('1', '6')
            AND (BOOK_BALANCE > 0 OR AVAILABLE_BALANCE > 0 OR OPENING_BALANCE > 0)
            FETCH FIRST 10 ROWS ONLY
            """
            
            cursor.execute(query1)
            results1 = cursor.fetchall()
            logger.info(f"Sample DEPOSIT_ACCOUNT records:")
            for i, row in enumerate(results1):
                account_number = row[0] if row[0] is not None else "NULL"
                logger.info(f"   {i+1}: Account={account_number} | Type={row[1]} | Status={row[2]} | Book={row[3]} | Available={row[4]} | Opening={row[5]}")
            
            # Test 2: Check for NULL ACCOUNT_NUMBER
            logger.info("🔍 Checking for NULL ACCOUNT_NUMBER...")
            
            query2 = """
            SELECT COUNT(*) as null_count
            FROM DEPOSIT_ACCOUNT 
            WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND ENTRY_STATUS IN ('1', '6')
            AND (BOOK_BALANCE > 0 OR AVAILABLE_BALANCE > 0 OR OPENING_BALANCE > 0)
            AND ACCOUNT_NUMBER IS NULL
            """
            
            cursor.execute(query2)
            null_count = cursor.fetchone()[0]
            logger.info(f"Records with NULL ACCOUNT_NUMBER: {null_count}")
            
            # Test 3: Check for empty/blank ACCOUNT_NUMBER
            logger.info("🔍 Checking for empty/blank ACCOUNT_NUMBER...")
            
            query3 = """
            SELECT COUNT(*) as blank_count
            FROM DEPOSIT_ACCOUNT 
            WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND ENTRY_STATUS IN ('1', '6')
            AND (BOOK_BALANCE > 0 OR AVAILABLE_BALANCE > 0 OR OPENING_BALANCE > 0)
            AND (ACCOUNT_NUMBER IS NULL OR TRIM(ACCOUNT_NUMBER) = '')
            """
            
            cursor.execute(query3)
            blank_count = cursor.fetchone()[0]
            logger.info(f"Records with NULL or blank ACCOUNT_NUMBER: {blank_count}")
            
            # Test 4: Check what we can use as alternative identifier
            logger.info("🔍 Checking alternative identifiers...")
            
            query4 = """
            SELECT 
                ACCOUNT_NUMBER,
                FK_CUSTOMERCUST_ID,
                FK_CURRENCYID_CURR,
                DEPOSIT_TYPE,
                OPENING_DATE
            FROM DEPOSIT_ACCOUNT 
            WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND ENTRY_STATUS IN ('1', '6')
            AND (BOOK_BALANCE > 0 OR AVAILABLE_BALANCE > 0 OR OPENING_BALANCE > 0)
            AND (ACCOUNT_NUMBER IS NULL OR TRIM(ACCOUNT_NUMBER) = '')
            FETCH FIRST 5 ROWS ONLY
            """
            
            cursor.execute(query4)
            results4 = cursor.fetchall()
            logger.info(f"Records with NULL/blank ACCOUNT_NUMBER (showing alternatives):")
            for i, row in enumerate(results4):
                logger.info(f"   {i+1}: Account={row[0]} | Customer={row[1]} | Currency={row[2]} | Type={row[3]} | Date={row[4]}")
            
            # Test 5: Check if we have a primary key or unique identifier
            logger.info("🔍 Checking for primary key columns...")
            
            query5 = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT FK_CUSTOMERCUST_ID) as unique_customers,
                COUNT(DISTINCT (FK_CUSTOMERCUST_ID || '-' || DEPOSIT_TYPE || '-' || COALESCE(CAST(OPENING_DATE AS VARCHAR(10)), 'NODATE'))) as unique_combinations
            FROM DEPOSIT_ACCOUNT 
            WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND ENTRY_STATUS IN ('1', '6')
            AND (BOOK_BALANCE > 0 OR AVAILABLE_BALANCE > 0 OR OPENING_BALANCE > 0)
            """
            
            cursor.execute(query5)
            result5 = cursor.fetchone()
            logger.info(f"Total records: {result5[0]}")
            logger.info(f"Unique customers: {result5[1]}")
            logger.info(f"Unique customer+type+date combinations: {result5[2]}")
            
    except Exception as e:
        logger.error(f"❌ Error in debug: {e}")
        raise

if __name__ == "__main__":
    debug_investment_data()