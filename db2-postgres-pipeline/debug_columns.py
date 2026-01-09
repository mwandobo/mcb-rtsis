#!/usr/bin/env python3
"""
Debug Investment Debt Securities column names
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_columns():
    """Debug investment debt securities column names"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test simple query first
            logger.info("🔍 Testing simple query...")
            
            query1 = """
            SELECT
                da.ACCOUNT_NUMBER,
                da.DEPOSIT_TYPE,
                da.BOOK_BALANCE
            FROM DEPOSIT_ACCOUNT da
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(query1)
            columns1 = [column[0] for column in cursor.description]
            logger.info(f"Simple query columns: {columns1}")
            
            for i, row in enumerate(cursor.fetchall()):
                record = dict(zip(columns1, row))
                logger.info(f"   Record {i+1}: {record}")
            
            # Test with CAST
            logger.info("🔍 Testing with CAST...")
            
            query2 = """
            SELECT
                CAST(da.ACCOUNT_NUMBER AS VARCHAR(50)) AS securityNumber,
                da.DEPOSIT_TYPE AS depositType,
                da.BOOK_BALANCE AS bookBalance
            FROM DEPOSIT_ACCOUNT da
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(query2)
            columns2 = [column[0] for column in cursor.description]
            logger.info(f"CAST query columns: {columns2}")
            
            for i, row in enumerate(cursor.fetchall()):
                record = dict(zip(columns2, row))
                logger.info(f"   Record {i+1}: {record}")
            
            # Test with CASE statement
            logger.info("🔍 Testing with CASE statement...")
            
            query3 = """
            SELECT
                CAST(da.ACCOUNT_NUMBER AS VARCHAR(50)) AS securityNumber,
                CASE 
                    WHEN da.DEPOSIT_TYPE = '1' THEN 'Corporate bonds'
                    WHEN da.DEPOSIT_TYPE = '2' THEN 'Treasury bonds'
                    ELSE 'Others'
                END AS securityType,
                da.BOOK_BALANCE AS bookBalance
            FROM DEPOSIT_ACCOUNT da
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(query3)
            columns3 = [column[0] for column in cursor.description]
            logger.info(f"CASE query columns: {columns3}")
            
            for i, row in enumerate(cursor.fetchall()):
                record = dict(zip(columns3, row))
                logger.info(f"   Record {i+1}: {record}")
            
    except Exception as e:
        logger.error(f"❌ Error in debug: {e}")
        raise

if __name__ == "__main__":
    debug_columns()