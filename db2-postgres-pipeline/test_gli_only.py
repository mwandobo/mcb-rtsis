#!/usr/bin/env python3
"""
Test Only GLI_TRX_EXTRACT Part of Investment Securities Query
"""

import pyodbc
import logging
from config import Config

def test_gli_only():
    """Test only the GLI_TRX_EXTRACT part of the investment securities query"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing GLI_TRX_EXTRACT Investment Securities Query")
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
        
        # First, test the subquery to see how many GL accounts match
        logger.info("📋 Testing GL account subquery...")
        
        gl_query = """
        SELECT COUNT(*) 
        FROM GLG_ACCOUNT 
        WHERE EXTERNAL_GLACCOUNT LIKE '130%'
        """
        
        cursor.execute(gl_query)
        gl_count = cursor.fetchone()[0]
        logger.info(f"📊 GL accounts with 130% pattern: {gl_count:,}")
        
        # Test GLI_TRX_EXTRACT with the subquery approach
        logger.info("📋 Testing GLI_TRX_EXTRACT with subquery...")
        
        gli_query = """
        SELECT
            CURRENT_TIMESTAMP AS reportingDate,
            (gte.FK_GLG_ACCOUNTACCO || '-' || COALESCE(CAST(gte.CUST_ID AS VARCHAR(10)), '0')) AS securityNumber,
            'Treasury bonds' AS securityType,
            'Government of Tanzania' AS securityIssuerName,
            'AAA' AS externalIssuerRatting,
            CAST(NULL AS VARCHAR(50)) AS gradesUnratedBanks,
            'Tanzania' AS securityIssuerCountry,
            'Central Government' AS snaIssuerSector,
            COALESCE(gte.CURRENCY_SHORT_DES, 'TZS') AS currency,
            gte.DC_AMOUNT AS orgCostValueAmount
        FROM GLI_TRX_EXTRACT gte
        WHERE gte.FK_GLG_ACCOUNTACCO IN (
            SELECT ACCOUNT_ID 
            FROM GLG_ACCOUNT 
            WHERE EXTERNAL_GLACCOUNT LIKE '130%'
        )
        AND gte.DC_AMOUNT IS NOT NULL
        AND gte.DC_AMOUNT > 0
        AND gte.TRN_DATE IS NOT NULL
        FETCH FIRST 5 ROWS ONLY
        """
        
        logger.info("📋 Executing GLI_TRX_EXTRACT query...")
        
        # Execute the query
        cursor.execute(gli_query)
        
        # Fetch results
        results = cursor.fetchall()
        
        logger.info(f"✅ GLI_TRX_EXTRACT query executed successfully!")
        logger.info(f"📊 Records returned: {len(results)}")
        
        if results:
            # Show sample records
            logger.info(f"\n📋 Sample records:")
            for i, row in enumerate(results):
                sec_num = row[1]  # securityNumber
                sec_type = row[2]  # securityType
                amount = row[9]  # orgCostValueAmount
                logger.info(f"  {i+1}. {sec_num} | {sec_type} | {amount:,.2f}")
        else:
            logger.info("⚠️ No records returned from GLI_TRX_EXTRACT query")
        
        # Test count query for GLI_TRX_EXTRACT
        logger.info(f"\n📋 Testing GLI_TRX_EXTRACT count query...")
        
        count_query = """
        SELECT COUNT(*) 
        FROM GLI_TRX_EXTRACT gte
        WHERE gte.FK_GLG_ACCOUNTACCO IN (
            SELECT ACCOUNT_ID 
            FROM GLG_ACCOUNT 
            WHERE EXTERNAL_GLACCOUNT LIKE '130%'
        )
        AND gte.DC_AMOUNT IS NOT NULL
        AND gte.DC_AMOUNT > 0
        AND gte.TRN_DATE IS NOT NULL
        """
        
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]
        logger.info(f"📊 Total GLI_TRX_EXTRACT investment records: {total_count:,}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ GLI_TRX_EXTRACT query execution failed: {e}")
        return False

if __name__ == "__main__":
    test_gli_only()