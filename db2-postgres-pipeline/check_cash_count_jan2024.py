#!/usr/bin/env python3
"""
Check how many cash records exist from January 2024
"""

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_cash_count():
    """Check count of cash records from January 2024"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Count query
            count_query = """
            SELECT COUNT(*) as total_records
            FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015')
            AND gte.TMSTAMP > TIMESTAMP('2024-01-01 00:00:00')
            """
            
            logger.info("🔍 Checking total cash records from January 2024...")
            cursor.execute(count_query)
            result = cursor.fetchone()
            
            total_records = result[0]
            logger.info(f"📊 Total cash records from Jan 2024: {total_records:,}")
            
            # Check by month
            monthly_query = """
            SELECT 
                YEAR(gte.TMSTAMP) as year,
                MONTH(gte.TMSTAMP) as month,
                COUNT(*) as monthly_count
            FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015')
            AND gte.TMSTAMP > TIMESTAMP('2024-01-01 00:00:00')
            GROUP BY YEAR(gte.TMSTAMP), MONTH(gte.TMSTAMP)
            ORDER BY year, month
            FETCH FIRST 12 ROWS ONLY
            """
            
            logger.info("📅 Checking monthly breakdown...")
            cursor.execute(monthly_query)
            rows = cursor.fetchall()
            
            logger.info("📊 Monthly cash record counts:")
            for row in rows:
                year, month, count = row
                logger.info(f"  {year}-{month:02d}: {count:,} records")
                
    except Exception as e:
        logger.error(f"❌ Error checking cash count: {e}")

if __name__ == "__main__":
    check_cash_count()