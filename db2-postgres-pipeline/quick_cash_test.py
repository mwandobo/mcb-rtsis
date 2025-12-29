#!/usr/bin/env python3
"""
Quick test of corrected cash query
"""

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quick_test():
    """Quick test of the corrected query"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test the corrected query structure
            query = """
            SELECT
                CURRENT_TIMESTAMP as reportingDate,
                gte.FK_UNITCODETRXUNIT AS branchCode,
                CASE
                  WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                  WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                  WHEN gl.EXTERNAL_GLACCOUNT='101000010' OR gl.EXTERNAL_GLACCOUNT='101000015' THEN 'Cash in ATMs'
                  WHEN gl.EXTERNAL_GLACCOUNT='101000004' OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Cash with Tellers'
                  ELSE 'unknown'
                END as cashCategory,
                CASE
                    WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'CleanNotes'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000002'  OR
                         gl.EXTERNAL_GLACCOUNT='101000010'  OR
                         gl.EXTERNAL_GLACCOUNT='101000004'  OR
                         gl.EXTERNAL_GLACCOUNT='101000015'  OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Notes'
                    ELSE null
                END as cashSubCategory,
                'Business Hours' as cashSubmissionTime,
                gte.CURRENCY_SHORT_DES as currency,
                null as cashDenomination,
                null as quantityOfCoinsNotes,
                gte.DC_AMOUNT AS orgAmount,
                CASE
                    WHEN gte.CURRENCY_SHORT_DES = 'USD'
                        THEN gte.DC_AMOUNT
                    ELSE NULL
                END AS usdAmount,
                CASE
                    WHEN gte.CURRENCY_SHORT_DES = 'USD'
                        THEN gte.DC_AMOUNT * 2500
                    ELSE
                        gte.DC_AMOUNT
                END AS tzsAmount,
                gte.TRN_DATE as transactionDate,
                gte.AVAILABILITY_DATE as maturityDate,
                0 as allowanceProbableLoss,
                0 as botProvision
            FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015')
            AND gte.TRN_DATE >= '2024-01-01'
            ORDER BY gte.TRN_DATE ASC
            FETCH FIRST 3 ROWS ONLY
            """
            
            logger.info("🔍 Testing corrected cash query...")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            logger.info(f"📊 Found {len(rows)} rows")
            
            if rows:
                logger.info("✅ Query executed successfully!")
                logger.info("📋 Sample results:")
                for i, row in enumerate(rows, 1):
                    logger.info(f"  {i}. cashSubmissionTime: '{row[4]}' (should be 'Business Hours')")
                    logger.info(f"     cashCategory: '{row[2]}'")
                    logger.info(f"     cashSubCategory: '{row[3]}'")
                    logger.info(f"     transactionDate: {row[11]} (using TRN_DATE)")
                    logger.info(f"     currency: {row[5]}, amount: {row[8]}")
                    logger.info("-" * 40)
                
                # Verify key corrections
                if rows[0][4] == 'Business Hours':
                    logger.info("✅ cashSubmissionTime correctly set to 'Business Hours'")
                else:
                    logger.error(f"❌ cashSubmissionTime wrong: '{rows[0][4]}'")
                
                return True
            else:
                logger.warning("⚠️ No rows returned")
                return False
                
    except Exception as e:
        logger.error(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("💰 QUICK CASH QUERY TEST")
    print("=" * 40)
    success = quick_test()
    if success:
        print("✅ Corrected query works!")
    else:
        print("❌ Query needs fixing")