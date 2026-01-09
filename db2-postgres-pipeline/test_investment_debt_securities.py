#!/usr/bin/env python3
"""
Test Investment Debt Securities data availability
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_investment_debt_securities():
    """Test investment debt securities data availability"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test 1: Check GLI_TRX_EXTRACT with 130% GL accounts
            logger.info("🔍 Testing GLI_TRX_EXTRACT with 130% GL accounts...")
            
            query1 = """
            SELECT COUNT(*) as count
            FROM GLI_TRX_EXTRACT gte
            LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gte.FK_GLG_ACCOUNTACCO IN (
                SELECT ACCOUNT_ID 
                FROM GLG_ACCOUNT 
                WHERE EXTERNAL_GLACCOUNT LIKE '130%'
            )
            AND gte.DC_AMOUNT IS NOT NULL
            AND gte.DC_AMOUNT > 0
            AND gte.TRN_DATE IS NOT NULL
            AND gte.TRN_DATE >= '2024-01-01'
            """
            
            cursor.execute(query1)
            result1 = cursor.fetchone()
            logger.info(f"   GLI_TRX_EXTRACT (130% GL accounts) from 2024: {result1[0]} records")
            
            # Test 2: Check DEPOSIT_ACCOUNT with DEPOSIT_TYPE 1-5
            logger.info("🔍 Testing DEPOSIT_ACCOUNT with DEPOSIT_TYPE 1-5...")
            
            query2 = """
            SELECT COUNT(*) as count
            FROM DEPOSIT_ACCOUNT da
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            """
            
            cursor.execute(query2)
            result2 = cursor.fetchone()
            logger.info(f"   DEPOSIT_ACCOUNT (DEPOSIT_TYPE 1-5): {result2[0]} records")
            
            # Test 3: Check GLG_ACCOUNT for 130% pattern
            logger.info("🔍 Testing GLG_ACCOUNT for 130% pattern...")
            
            query3 = """
            SELECT COUNT(*) as count
            FROM GLG_ACCOUNT 
            WHERE EXTERNAL_GLACCOUNT LIKE '130%'
            """
            
            cursor.execute(query3)
            result3 = cursor.fetchone()
            logger.info(f"   GLG_ACCOUNT with 130% pattern: {result3[0]} accounts")
            
            # Test 4: Sample GLG_ACCOUNT with 130% pattern
            if result3[0] > 0:
                logger.info("🔍 Sample GLG_ACCOUNT with 130% pattern...")
                
                query4 = """
                SELECT ACCOUNT_ID, EXTERNAL_GLACCOUNT, ACCOUNT_NAME
                FROM GLG_ACCOUNT 
                WHERE EXTERNAL_GLACCOUNT LIKE '130%'
                FETCH FIRST 5 ROWS ONLY
                """
                
                cursor.execute(query4)
                results4 = cursor.fetchall()
                for row in results4:
                    logger.info(f"   Account: {row[0]} | GL: {row[1]} | Name: {row[2]}")
            
            # Test 5: Sample DEPOSIT_ACCOUNT by type
            logger.info("🔍 Sample DEPOSIT_ACCOUNT by DEPOSIT_TYPE...")
            
            query5 = """
            SELECT DEPOSIT_TYPE, COUNT(*) as count
            FROM DEPOSIT_ACCOUNT da
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            GROUP BY DEPOSIT_TYPE
            ORDER BY DEPOSIT_TYPE
            """
            
            cursor.execute(query5)
            results5 = cursor.fetchall()
            for row in results5:
                deposit_type_name = {
                    '1': 'Corporate bonds',
                    '2': 'Treasury bonds', 
                    '3': 'Treasury bills',
                    '4': 'RGOZ Treasury bond',
                    '5': 'Municipal/Local Government bond'
                }.get(row[0], f'Type {row[0]}')
                logger.info(f"   {deposit_type_name}: {row[1]} records")
            
            # Test 6: Try a simple version of the first part of UNION query
            logger.info("🔍 Testing simple GLI_TRX_EXTRACT query...")
            
            query6 = """
            SELECT 
                CURRENT_TIMESTAMP AS reportingDate,
                (gte.FK_GLG_ACCOUNTACCO || '-' || COALESCE(CAST(gte.CUST_ID AS VARCHAR(10)), '0')) AS securityNumber,
                'Treasury bonds' AS securityType,
                gte.DC_AMOUNT AS orgCostValueAmount
            FROM GLI_TRX_EXTRACT gte
            LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gte.FK_GLG_ACCOUNTACCO IN (
                SELECT ACCOUNT_ID 
                FROM GLG_ACCOUNT 
                WHERE EXTERNAL_GLACCOUNT LIKE '130%'
            )
            AND gte.DC_AMOUNT IS NOT NULL
            AND gte.DC_AMOUNT > 0
            AND gte.TRN_DATE IS NOT NULL
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(query6)
            results6 = cursor.fetchall()
            logger.info(f"   Simple GLI_TRX_EXTRACT query returned {len(results6)} records")
            for i, row in enumerate(results6):
                logger.info(f"   Sample {i+1}: {row[1]} | {row[2]} | Amount: {row[3]}")
            
            # Test 7: Try a simple version of the second part of UNION query  
            logger.info("🔍 Testing simple DEPOSIT_ACCOUNT query...")
            
            query7 = """
            SELECT 
                CURRENT_TIMESTAMP AS reportingDate,
                CAST(da.ACCOUNT_NUMBER AS VARCHAR(50)) AS securityNumber,
                CASE 
                    WHEN da.DEPOSIT_TYPE = '1' THEN 'Corporate bonds'
                    WHEN da.DEPOSIT_TYPE = '2' THEN 'Treasury bonds'
                    WHEN da.DEPOSIT_TYPE = '3' THEN 'Treasury bills'
                    WHEN da.DEPOSIT_TYPE = '4' THEN 'RGOZ Treasury bond'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Municipal/Local Government bond'
                    ELSE 'Others investments'
                END AS securityType,
                COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount
            FROM DEPOSIT_ACCOUNT da
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(query7)
            results7 = cursor.fetchall()
            logger.info(f"   Simple DEPOSIT_ACCOUNT query returned {len(results7)} records")
            for i, row in enumerate(results7):
                logger.info(f"   Sample {i+1}: {row[1]} | {row[2]} | Amount: {row[3]}")
            
    except Exception as e:
        logger.error(f"❌ Error testing investment debt securities: {e}")
        raise

if __name__ == "__main__":
    test_investment_debt_securities()