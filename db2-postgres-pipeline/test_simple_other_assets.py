#!/usr/bin/env python3
"""
Simple test for Other Assets - just check if we can fetch data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_simple_other_assets():
    """Simple test to fetch other assets data"""
    
    # Simplified query to test basic functionality
    simple_query = """
    SELECT
        CURRENT_TIMESTAMP AS reportingDate,
        'Gold' AS assetType,
        gte.TRN_DATE AS transactionDate,
        gte.AVAILABILITY_DATE AS maturityDate,
        'Test Debtor' AS debtorName,
        'Tanzania' AS debtorCountry,
        gte.CURRENCY_SHORT_DES AS currency,
        gte.DC_AMOUNT AS orgAmount,
        CASE WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT ELSE NULL END AS usdAmount,
        CASE WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50 ELSE gte.DC_AMOUNT END AS tzsAmount,
        'Other Non-Financial Corporations' AS sectorSnaClassification,
        0 AS pastDueDays,
        1 AS assetClassificationCategory,
        0 AS allowanceProbableLoss,
        0 AS botProvision
    FROM GLI_TRX_EXTRACT AS gte
    LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gl.EXTERNAL_GLACCOUNT IN (
        '100017000','144000032','144000015','144000047','144000048','144000050',
        '144000051','144000054','144000058','144000061','144000062','144000074',
        '230000007','230000071','145000001','230000079','144000006','144000066'
    )
    FETCH FIRST 10 ROWS ONLY
    """
    
    try:
        logger.info("Testing simple other assets query...")
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(simple_query)
            rows = cursor.fetchall()
            
            logger.info(f"Fetched {len(rows)} records")
            
            for i, row in enumerate(rows):
                logger.info(f"Record {i+1}: Asset Type: {row[1]}, Amount: {row[7]}, Currency: {row[6]}")
                
        logger.info("Simple test completed successfully!")
        
    except Exception as e:
        logger.error(f"Simple test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_other_assets()