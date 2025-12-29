#!/usr/bin/env python3
"""
Simple cash data check
"""

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_cash_data():
    """Check if cash data exists"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Simple count query first
            count_query = """
            SELECT COUNT(*) 
            FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
            AND gte.TRN_DATE >= '2024-01-01'
            """
            
            logger.info("🔍 Checking cash data count...")
            cursor.execute(count_query)
            count = cursor.fetchone()[0]
            logger.info(f"📊 Total cash records from 2024-01-01: {count:,}")
            
            if count > 0:
                # Get sample data
                sample_query = """
                SELECT 
                    gte.TRN_DATE,
                    gte.FK_UNITCODETRXUNIT,
                    gte.CURRENCY_SHORT_DES,
                    gte.DC_AMOUNT
                FROM GLI_TRX_EXTRACT gte 
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
                AND gte.TRN_DATE >= '2024-01-01'
                ORDER BY gte.TRN_DATE ASC
                FETCH FIRST 3 ROWS ONLY
                """
                
                logger.info("📋 Getting sample data...")
                cursor.execute(sample_query)
                rows = cursor.fetchall()
                
                for i, row in enumerate(rows, 1):
                    logger.info(f"Sample {i}: Date={row[0]}, Branch={row[1]}, Currency={row[2]}, Amount={row[3]}")
                    logger.info(f"  Types: {type(row[0])}, {type(row[1])}, {type(row[2])}, {type(row[3])}")
            
    except Exception as e:
        logger.error(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_cash_data()