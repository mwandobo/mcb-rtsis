#!/usr/bin/env python3
"""
Test Investment Debt Securities Query Parts Separately
"""

import pyodbc
import logging
from config import Config

def test_query_parts():
    """Test each part of the investment debt securities query separately"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing Investment Debt Securities Query Parts")
    logger.info("=" * 50)
    
    try:
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
        )
        
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        
        # Test 1: GLI_TRX_EXTRACT part (first part of UNION)
        logger.info("\n📋 Test 1: GLI_TRX_EXTRACT part...")
        
        gli_query = """
        SELECT COUNT(*) as record_count
        FROM GLI_TRX_EXTRACT gte
        LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        LEFT JOIN CUSTOMER c ON gte.CUST_ID = c.CUST_ID
        WHERE gl.EXTERNAL_GLACCOUNT LIKE '130%'
            AND gte.DC_AMOUNT IS NOT NULL
            AND gte.DC_AMOUNT > 0
            AND gte.TRN_DATE IS NOT NULL
        """
        
        try:
            cursor.execute(gli_query)
            result = cursor.fetchone()
            logger.info(f"✅ GLI_TRX_EXTRACT part: {result[0]:,} records would be returned")
        except Exception as e:
            logger.error(f"❌ GLI_TRX_EXTRACT part failed: {e}")
        
        # Test 2: DEPOSIT_ACCOUNT part (second part of UNION)
        logger.info("\n📋 Test 2: DEPOSIT_ACCOUNT part...")
        
        deposit_query = """
        SELECT COUNT(*) as record_count
        FROM DEPOSIT_ACCOUNT da
        LEFT JOIN CUSTOMER c ON da.FK_CUSTOMERCUST_ID = c.CUST_ID
        LEFT JOIN CURRENCY cur ON da.FK_CURRENCYID_CURR = cur.ID_CURRENCY
        WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
        """
        
        try:
            cursor.execute(deposit_query)
            result = cursor.fetchone()
            logger.info(f"✅ DEPOSIT_ACCOUNT part: {result[0]:,} records would be returned")
        except Exception as e:
            logger.error(f"❌ DEPOSIT_ACCOUNT part failed: {e}")
        
        # Test 3: Simple version of GLI_TRX_EXTRACT with minimal columns
        logger.info("\n📋 Test 3: Simple GLI_TRX_EXTRACT query...")
        
        simple_gli_query = """
        SELECT 
            CURRENT_TIMESTAMP AS reportingDate,
            (gte.FK_GLG_ACCOUNTACCO || '-' || COALESCE(CAST(gte.CUST_ID AS VARCHAR(10)), '0')) AS securityNumber,
            'Treasury bonds' AS securityType,
            gte.DC_AMOUNT AS orgCostValueAmount
        FROM GLI_TRX_EXTRACT gte
        LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        WHERE gl.EXTERNAL_GLACCOUNT LIKE '130%'
            AND gte.DC_AMOUNT IS NOT NULL
            AND gte.DC_AMOUNT > 0
            AND gte.TRN_DATE IS NOT NULL
        FETCH FIRST 3 ROWS ONLY
        """
        
        try:
            cursor.execute(simple_gli_query)
            results = cursor.fetchall()
            logger.info(f"✅ Simple GLI_TRX_EXTRACT: {len(results)} sample records")
            for i, row in enumerate(results):
                logger.info(f"  Record {i+1}: {row[1]}, {row[3]}")  # securityNumber, amount
        except Exception as e:
            logger.error(f"❌ Simple GLI_TRX_EXTRACT failed: {e}")
        
        # Test 4: Simple version of DEPOSIT_ACCOUNT with minimal columns
        logger.info("\n📋 Test 4: Simple DEPOSIT_ACCOUNT query...")
        
        simple_deposit_query = """
        SELECT 
            CURRENT_TIMESTAMP AS reportingDate,
            CAST(da.ACCOUNT_NUMBER AS VARCHAR(50)) AS securityNumber,
            CASE 
                WHEN da.DEPOSIT_TYPE = '1' THEN 'Corporate bonds'
                WHEN da.DEPOSIT_TYPE = '2' THEN 'Treasury bonds'
                WHEN da.DEPOSIT_TYPE = '3' THEN 'Treasury bills'
                WHEN da.DEPOSIT_TYPE = '4' THEN 'RGOZ Treasury bond'
                WHEN da.DEPOSIT_TYPE = '5' THEN 'Municipal/Local Government bond'
                ELSE 'Others investments (Specify name)'
            END AS securityType,
            COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount
        FROM DEPOSIT_ACCOUNT da
        WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
        FETCH FIRST 3 ROWS ONLY
        """
        
        try:
            cursor.execute(simple_deposit_query)
            results = cursor.fetchall()
            logger.info(f"✅ Simple DEPOSIT_ACCOUNT: {len(results)} sample records")
            for i, row in enumerate(results):
                logger.info(f"  Record {i+1}: {row[1]}, {row[2]}, {row[3]}")  # securityNumber, type, amount
        except Exception as e:
            logger.error(f"❌ Simple DEPOSIT_ACCOUNT failed: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_query_parts()