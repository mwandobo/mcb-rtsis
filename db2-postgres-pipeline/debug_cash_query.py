#!/usr/bin/env python3
"""
Debug cash query to see what data is returned
"""

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_cash_query():
    """Test the cash query to see data structure"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test query with manual start date
            query = """
            SELECT 
                CURRENT_TIMESTAMP AS REPORTINGDATE,
                gte.FK_UNITCODETRXUNIT AS BRANCHCODE,
                CASE 
                    WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                    WHEN gl.EXTERNAL_GLACCOUNT IN ('101000010','101000015') THEN 'Cash in ATMs'
                    WHEN gl.EXTERNAL_GLACCOUNT IN ('101000004','101000011') THEN 'Cash in Teller'
                    ELSE 'Other cash'
                END AS CASHCATEGORY,
                NULL AS CASHSUBCATEGORY,
                CURRENT_TIMESTAMP AS CASHSUBMISSIONTIME,
                gte.CURRENCY_SHORT_DES AS CURRENCY,
                NULL AS CASHDENOMINATION,
                NULL AS QUANTITYOFCOINSNOTES,
                gte.DC_AMOUNT AS ORGAMOUNT,
                CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT ELSE NULL END AS USDAMOUNT,
                CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT*2500 ELSE gte.DC_AMOUNT END AS TZSAMOUNT,
                gte.TRN_DATE AS TRANSACTIONDATE,
                gte.AVAILABILITY_DATE AS MATURITYDATE,
                CAST(0 AS DECIMAL(18,2)) AS ALLOWANCEPROBABLELOSS,
                CAST(0 AS DECIMAL(18,2)) AS BOTPROVISSION
            FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
            AND gte.TRN_DATE >= TIMESTAMP('2024-01-01 00:00:00')
            ORDER BY gte.TRN_DATE ASC
            FETCH FIRST 3 ROWS ONLY
            """
            
            logger.info("🔍 Testing cash query...")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            logger.info(f"📊 Found {len(rows)} rows")
            
            if rows:
                logger.info("📋 Column structure:")
                for i, row in enumerate(rows):
                    logger.info(f"Row {i+1}:")
                    for j, value in enumerate(row):
                        logger.info(f"  Column {j}: {type(value).__name__} = {value}")
                    logger.info("-" * 50)
            else:
                logger.info("⚠️ No rows returned")
                
    except Exception as e:
        logger.error(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_cash_query()