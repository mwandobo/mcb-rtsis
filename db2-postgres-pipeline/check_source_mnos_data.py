#!/usr/bin/env python3
"""
Check source MNOs data to understand the data patterns
"""

from db2_connection import DB2Connection
import logging
from contextlib import contextmanager

def check_source_mnos_data():
    """Check the source MNOs data patterns"""
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
        logger.info("🔍 CHECKING SOURCE MNOS DATA PATTERNS")
        logger.info("=" * 60)
        
        with get_db2_connection() as conn:
            cursor = conn.cursor()
            
            # Check distinct combinations
            query = """
            SELECT 
                gl.EXTERNAL_GLACCOUNT,
                CASE gl.EXTERNAL_GLACCOUNT
                    WHEN '504080001' THEN 'Super Agent Commission'
                    WHEN '144000051' THEN 'AIRTEL Money Super Agent Float'
                    WHEN '144000058' THEN 'TIGO PESA Super Agent Float'
                    WHEN '144000061' THEN 'HALOPESA Super Agent Float'
                    WHEN '144000062' THEN 'MPESA Super Agent Float'
                    ELSE ''
                END AS mnoCode,
                gte.FK_GLG_ACCOUNTACCO AS tillNumber,
                COUNT(*) as record_count,
                MIN(gte.TRN_DATE) as earliest_date,
                MAX(gte.TRN_DATE) as latest_date,
                SUM(gte.DC_AMOUNT) as total_amount
            FROM GLI_TRX_EXTRACT gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('504080001','144000051','144000058','144000061','144000062')
                AND gte.TRN_DATE >= '2024-01-01'
            GROUP BY gl.EXTERNAL_GLACCOUNT, gte.FK_GLG_ACCOUNTACCO
            ORDER BY record_count DESC
            FETCH FIRST 20 ROWS ONLY
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"📊 DISTINCT COMBINATIONS (Top 20):")
            logger.info("GL Account | MNO Code | Till Number | Count | Earliest | Latest | Total Amount")
            logger.info("-" * 100)
            
            for row in results:
                gl_account, mno_code, till_number, count, earliest, latest, total = row
                logger.info(f"{gl_account} | {mno_code[:20]:<20} | {till_number} | {count:>5} | {earliest} | {latest} | {total:>12.2f}")
            
            # Check sample records for one combination
            logger.info("\n🔍 SAMPLE RECORDS FOR TIGO PESA:")
            sample_query = """
            SELECT 
                gte.TRN_DATE,
                gte.FK_GLG_ACCOUNTACCO,
                gte.DC_AMOUNT,
                gte.CURRENCY_SHORT_DES
            FROM GLI_TRX_EXTRACT gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT = '144000058'
                AND gte.TRN_DATE >= '2024-01-01'
            ORDER BY gte.TRN_DATE DESC
            FETCH FIRST 10 ROWS ONLY
            """
            
            cursor.execute(sample_query)
            sample_results = cursor.fetchall()
            
            logger.info("Date | Till Number | Amount | Currency")
            logger.info("-" * 50)
            for row in sample_results:
                trn_date, till_number, amount, currency = row
                logger.info(f"{trn_date} | {till_number} | {amount:>10.2f} | {currency}")
            
    except Exception as e:
        logger.error(f"❌ Failed to check source MNOs data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_source_mnos_data()