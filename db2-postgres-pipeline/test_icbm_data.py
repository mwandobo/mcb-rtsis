#!/usr/bin/env python3
"""
Test ICBM Data Availability
"""

from db2_connection import DB2Connection
import logging

def test_icbm_data():
    db2_conn = DB2Connection()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Test 1: Check if GL account 102000001 exists
            logger.info("Testing GL account 102000001...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM GLG_ACCOUNT 
                WHERE EXTERNAL_GLACCOUNT = '102000001'
            """)
            gl_account_exists = cursor.fetchone()[0]
            logger.info(f"GL account 102000001 exists: {gl_account_exists > 0}")
            
            # Test 2: Check all GL accounts starting with 102
            logger.info("Checking GL accounts starting with 102...")
            cursor.execute("""
                SELECT EXTERNAL_GLACCOUNT, COUNT(*) as transaction_count
                FROM GLG_ACCOUNT gl
                LEFT JOIN GLI_TRX_EXTRACT gte ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                WHERE gl.EXTERNAL_GLACCOUNT LIKE '102%'
                GROUP BY gl.EXTERNAL_GLACCOUNT
                ORDER BY gl.EXTERNAL_GLACCOUNT
                FETCH FIRST 10 ROWS ONLY
            """)
            gl_accounts = cursor.fetchall()
            logger.info("GL accounts starting with 102:")
            for account, count in gl_accounts:
                logger.info(f"  {account}: {count} transactions")
            
            # Test 3: Check any transactions in GLI_TRX_EXTRACT for accounts starting with 102
            logger.info("Checking transactions in accounts starting with 102...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM GLI_TRX_EXTRACT gte
                JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                WHERE gl.EXTERNAL_GLACCOUNT LIKE '102%'
            """)
            total_102_transactions = cursor.fetchone()[0]
            logger.info(f"Total transactions in 102xxx accounts: {total_102_transactions}")
            
            # Test 4: Check sample transactions if any exist
            if total_102_transactions > 0:
                logger.info("Sample transactions from 102xxx accounts:")
                cursor.execute("""
                    SELECT gl.EXTERNAL_GLACCOUNT, gte.TRN_DATE, gte.DC_AMOUNT
                    FROM GLI_TRX_EXTRACT gte
                    JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                    WHERE gl.EXTERNAL_GLACCOUNT LIKE '102%'
                    ORDER BY gte.TRN_DATE DESC
                    FETCH FIRST 5 ROWS ONLY
                """)
                sample_transactions = cursor.fetchall()
                for account, date, amount in sample_transactions:
                    logger.info(f"  {account} | {date} | {amount}")
            
    except Exception as e:
        logger.error(f"Error testing ICBM data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_icbm_data()