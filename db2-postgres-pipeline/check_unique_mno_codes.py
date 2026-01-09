#!/usr/bin/env python3
"""
Check unique MNO codes in source data
"""

from db2_connection import DB2Connection
import logging
from contextlib import contextmanager

def check_unique_mno_codes():
    """Check unique MNO codes in the source data"""
    db2_conn = DB2Connection()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_db2_connection():
        """Get DB2 connection"""
        with db2_conn.get_connection() as conn:
            yield conn
    
    try:
        logger.info("🔍 CHECKING UNIQUE MNO CODES IN SOURCE DATA")
        logger.info("=" * 60)
        
        with get_db2_connection() as conn:
            cursor = conn.cursor()
            
            # Check all unique MNO codes with their GL accounts
            query = """
            SELECT 
                gl.EXTERNAL_GLACCOUNT,
                CASE gl.EXTERNAL_GLACCOUNT
                    WHEN '504080001' THEN 'Super Agent Commission'
                    WHEN '144000051' THEN 'AIRTEL Money Super Agent Float'
                    WHEN '144000058' THEN 'TIGO PESA Super Agent Float'
                    WHEN '144000061' THEN 'HALOPESA Super Agent Float'
                    WHEN '144000062' THEN 'MPESA Super Agent Float'
                    ELSE 'Unknown'
                END AS mnoCode,
                COUNT(*) as total_transactions,
                COUNT(DISTINCT gte.FK_GLG_ACCOUNTACCO) as unique_tills,
                MIN(gte.TRN_DATE) as earliest_date,
                MAX(gte.TRN_DATE) as latest_date,
                SUM(gte.DC_AMOUNT) as total_amount
            FROM GLI_TRX_EXTRACT gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('504080001','144000051','144000058','144000061','144000062')
            GROUP BY gl.EXTERNAL_GLACCOUNT
            ORDER BY total_transactions DESC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"📊 UNIQUE MNO CODES FOUND: {len(results)}")
            logger.info("=" * 80)
            logger.info("GL Account | MNO Code                     | Transactions | Tills | Earliest   | Latest     | Total Amount")
            logger.info("-" * 80)
            
            total_transactions = 0
            total_amount = 0
            
            for row in results:
                gl_account, mno_code, transactions, tills, earliest, latest, amount = row
                total_transactions += transactions
                total_amount += amount
                logger.info(f"{gl_account} | {mno_code:<28} | {transactions:>12} | {tills:>5} | {earliest} | {latest} | {amount:>15,.2f}")
            
            logger.info("-" * 80)
            logger.info(f"TOTAL      | {len(results)} MNO Codes                | {total_transactions:>12} | -     | -          | -          | {total_amount:>15,.2f}")
            
            # Check if there are any records from 2024 onwards for each MNO
            logger.info("\n🕒 CHECKING 2024+ DATA FOR EACH MNO:")
            logger.info("-" * 60)
            
            for row in results:
                gl_account, mno_code, _, _, _, _, _ = row
                
                recent_query = """
                SELECT COUNT(*) as recent_count,
                       MAX(gte.TRN_DATE) as latest_recent_date,
                       SUM(gte.DC_AMOUNT) as recent_amount
                FROM GLI_TRX_EXTRACT gte
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                WHERE gl.EXTERNAL_GLACCOUNT = ?
                  AND gte.TRN_DATE >= '2024-01-01'
                """
                
                cursor.execute(recent_query, (gl_account,))
                recent_result = cursor.fetchone()
                recent_count, latest_recent, recent_amount = recent_result
                
                logger.info(f"{mno_code:<28} | 2024+ Records: {recent_count:>6} | Latest: {latest_recent} | Amount: {recent_amount:>12,.2f}")
            
    except Exception as e:
        logger.error(f"❌ Failed to check unique MNO codes: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_unique_mno_codes()