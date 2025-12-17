#!/usr/bin/env python3
"""
Test Only DEPOSIT_ACCOUNT Part of Investment Securities Query
"""

import pyodbc
import logging
from config import Config

def test_deposit_only():
    """Test only the DEPOSIT_ACCOUNT part of the investment securities query"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing DEPOSIT_ACCOUNT Investment Securities Query")
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
        
        # Simple DEPOSIT_ACCOUNT query
        query = """
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
            'Government of Tanzania' AS securityIssuerName,
            'AAA' AS externalIssuerRatting,
            CAST(NULL AS VARCHAR(50)) AS gradesUnratedBanks,
            'Tanzania' AS securityIssuerCountry,
            'Central Government' AS snaIssuerSector,
            'TZS' AS currency,
            COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount
        FROM DEPOSIT_ACCOUNT da
        WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
        FETCH FIRST 5 ROWS ONLY
        """
        
        logger.info("📋 Executing DEPOSIT_ACCOUNT query...")
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch results
        results = cursor.fetchall()
        
        logger.info(f"✅ DEPOSIT_ACCOUNT query executed successfully!")
        logger.info(f"📊 Records returned: {len(results)}")
        
        if results:
            # Show column names
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"\n📋 Columns:")
            for i, col in enumerate(columns):
                logger.info(f"  {i+1:2d}. {col}")
            
            # Show sample records
            logger.info(f"\n📋 Sample records:")
            for i, row in enumerate(results):
                sec_num = row[1]  # securityNumber
                sec_type = row[2]  # securityType
                amount = row[9]  # orgCostValueAmount
                logger.info(f"  {i+1}. {sec_num} | {sec_type} | {amount:,.2f}")
        else:
            logger.info("⚠️ No records returned from DEPOSIT_ACCOUNT query")
        
        # Test count query
        logger.info(f"\n📋 Testing count query...")
        
        count_query = """
        SELECT COUNT(*) 
        FROM DEPOSIT_ACCOUNT da
        WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
        """
        
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]
        logger.info(f"📊 Total DEPOSIT_ACCOUNT investment records: {total_count:,}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ DEPOSIT_ACCOUNT query execution failed: {e}")
        return False

if __name__ == "__main__":
    test_deposit_only()