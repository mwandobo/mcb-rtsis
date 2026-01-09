#!/usr/bin/env python3
"""
Test column mapping for investment debt securities
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_column_mapping():
    """Test column mapping for investment debt securities"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Use the exact query from simple_investment_pipeline.py
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
                CASE 
                    WHEN da.DEPOSIT_TYPE = '2' THEN 'Government of Tanzania'
                    WHEN da.DEPOSIT_TYPE = '3' THEN 'Bank of Tanzania'
                    WHEN da.DEPOSIT_TYPE = '4' THEN 'Government of Tanzania'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government Authority'
                    ELSE 'Unknown Issuer'
                END AS securityIssuerName,
                CASE 
                    WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'AAA'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'A'
                    ELSE NULL
                END AS externalIssuerRatting,
                NULL AS gradesUnratedBanks,
                'Tanzania' AS securityIssuerCountry,
                CASE 
                    WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Central Government'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government'
                    ELSE 'Other Non-Financial Corporations'
                END AS snaIssuerSector,
                COALESCE(cur.SHORT_DESCR, 'TZS') AS currency,
                COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
                    ELSE COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
                END AS tzsCostValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
                    WHEN cur.SHORT_DESCR = 'TZS'
                        THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
                    ELSE NULL
                END AS usdCostValueAmount,
                COALESCE(da.BOOK_BALANCE, 0) AS orgFaceValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.BOOK_BALANCE, 0) * 2730.50
                    ELSE COALESCE(da.BOOK_BALANCE, 0)
                END AS tzsgFaceValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.BOOK_BALANCE, 0)
                    WHEN cur.SHORT_DESCR = 'TZS'
                        THEN COALESCE(da.BOOK_BALANCE, 0) / 2730.50
                    ELSE NULL
                END AS usdgFaceValueAmount,
                COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) AS orgFairValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
                    ELSE COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
                END AS tzsgFairValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
                    WHEN cur.SHORT_DESCR = 'TZS'
                        THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
                    ELSE NULL
                END AS usdgFairValueAmount,
                COALESCE(da.FIXED_INTER_RATE, 0) AS interestRate,
                da.OPENING_DATE AS purchaseDate,
                COALESCE(da.START_DATE_TD, da.OPENING_DATE) AS valueDate,
                COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) AS maturityDate,
                CASE 
                    WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Hold to Maturity'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Hold to Maturity'
                    ELSE 'Available for Sale'
                END AS tradingIntent,
                CASE 
                    WHEN da.COLLATERAL_FLG = '1' THEN 'Encumbered'
                    ELSE 'Unencumbered'
                END AS securityEncumbaranceStatus,
                CASE 
                    WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NOT NULL 
                         AND COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) < CURRENT_DATE
                        THEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE))
                    ELSE 0
                END AS pastDueDays,
                CAST(0 AS DECIMAL(15, 2)) AS allowanceProbableLoss,
                CASE 
                    WHEN da.ENTRY_STATUS NOT IN ('1', '6') THEN 5
                    WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NULL 
                         OR COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) >= CURRENT_DATE
                        THEN 1
                    WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 90
                        THEN 2
                    WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 180
                        THEN 3
                    WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 365
                        THEN 4
                    ELSE 5
                END AS assetClassificationCategory
            FROM DEPOSIT_ACCOUNT da
            LEFT JOIN CUSTOMER c ON da.FK_CUSTOMERCUST_ID = c.CUST_ID
            LEFT JOIN CURRENCY cur ON da.FK_CURRENCYID_CURR = cur.ID_CURRENCY
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            FETCH FIRST 2 ROWS ONLY
            """
            
            cursor.execute(query)
            
            # Get column names (DB2 returns them in uppercase)
            db2_columns = [column[0] for column in cursor.description]
            lowercase_columns = [column[0].lower() for column in cursor.description]
            
            logger.info(f"DB2 column names (uppercase): {db2_columns}")
            logger.info(f"Lowercase column names: {lowercase_columns}")
            
            for i, row in enumerate(cursor.fetchall()):
                record = dict(zip(lowercase_columns, row))
                logger.info(f"Record {i+1} sample fields:")
                logger.info(f"   securitynumber: '{record.get('securitynumber')}' (type: {type(record.get('securitynumber'))})")
                logger.info(f"   securitytype: '{record.get('securitytype')}'")
                logger.info(f"   orgcostvalueamount: '{record.get('orgcostvalueamount')}'")
                logger.info("")
            
    except Exception as e:
        logger.error(f"❌ Error in test: {e}")
        raise

if __name__ == "__main__":
    test_column_mapping()