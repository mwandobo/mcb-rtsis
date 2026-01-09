#!/usr/bin/env python3
"""
Debug Investment Debt Securities processor
"""

import logging
from db2_connection import DB2Connection
from processors.investment_debt_securities_processor import InvestmentDebtSecuritiesProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_processor():
    """Debug investment debt securities processor"""
    
    db2_conn = DB2Connection()
    processor = InvestmentDebtSecuritiesProcessor()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get a simple sample record
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
                    ELSE 'Others investments'
                END AS securityType,
                'Government of Tanzania' AS securityIssuerName,
                'AAA' AS externalIssuerRatting,
                NULL AS gradesUnratedBanks,
                'Tanzania' AS securityIssuerCountry,
                'Central Government' AS snaIssuerSector,
                'TZS' AS currency,
                COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount,
                COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS tzsCostValueAmount,
                NULL AS usdCostValueAmount,
                COALESCE(da.BOOK_BALANCE, 0) AS orgFaceValueAmount,
                COALESCE(da.BOOK_BALANCE, 0) AS tzsgFaceValueAmount,
                NULL AS usdgFaceValueAmount,
                COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) AS orgFairValueAmount,
                COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) AS tzsgFairValueAmount,
                NULL AS usdgFairValueAmount,
                COALESCE(da.FIXED_INTER_RATE, 0) AS interestRate,
                da.OPENING_DATE AS purchaseDate,
                da.OPENING_DATE AS valueDate,
                da.EXPIRY_DATE AS maturityDate,
                'Hold to Maturity' AS tradingIntent,
                'Unencumbered' AS securityEncumbaranceStatus,
                0 AS pastDueDays,
                CAST(0 AS DECIMAL(15, 2)) AS allowanceProbableLoss,
                1 AS assetClassificationCategory
            FROM DEPOSIT_ACCOUNT da
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            
            for i, row in enumerate(cursor.fetchall()):
                record = dict(zip(columns, row))
                
                logger.info(f"🔍 Processing record {i+1}:")
                logger.info(f"   Raw securityNumber: '{record.get('securityNumber')}' (type: {type(record.get('securityNumber'))})")
                logger.info(f"   Raw securityType: '{record.get('securityType')}'")
                logger.info(f"   Raw orgCostValueAmount: '{record.get('orgCostValueAmount')}'")
                
                try:
                    # Process the record
                    processed_record = processor.process_record(record)
                    
                    logger.info(f"   Processed securityNumber: '{processed_record.get('securityNumber')}' (type: {type(processed_record.get('securityNumber'))})")
                    logger.info(f"   Processed securityType: '{processed_record.get('securityType')}'")
                    logger.info(f"   Processed orgCostValueAmount: '{processed_record.get('orgCostValueAmount')}'")
                    
                    # Get insert parameters
                    params = processor.get_insert_params(processed_record)
                    logger.info(f"   Insert params[1] (securityNumber): '{params[1]}' (type: {type(params[1])})")
                    
                except Exception as e:
                    logger.error(f"   ❌ Error processing record: {e}")
                    logger.error(f"   Record: {record}")
                
                logger.info("")
            
    except Exception as e:
        logger.error(f"❌ Error in debug: {e}")
        raise

if __name__ == "__main__":
    debug_processor()