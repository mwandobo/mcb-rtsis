"""
Test loan transaction query to identify issues
"""

import logging
from config import Config
from db2_connection import DB2Connection
import pyodbc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_query():
    """Test the loan transaction query"""
    config = Config()
    
    # Simple test query first
    test_query = """
    SELECT CURRENT_TIMESTAMP AS reportingDate,
           gte.PRF_ACCOUNT_NUMBER AS loanNumber,
           gte.TRN_DATE AS transactionDate,
           gte.CURRENCY_SHORT_DES AS currency,
           gte.DC_AMOUNT AS orgTransactionAmount
    FROM GLI_TRX_EXTRACT AS gte
    LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    WHERE gl.EXTERNAL_GLACCOUNT IN (
        '110000001', '110000005', '110010001', '110010012'
    )
    AND gte.PRF_ACCOUNT_NUMBER IS NOT NULL
    FETCH FIRST 10 ROWS ONLY
    """
    
    try:
        logger.info("Connecting to DB2...")
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"PROTOCOL=TCPIP;"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
            f"CURRENTSCHEMA={config.database.db2_schema};"
        )
        conn = pyodbc.connect(conn_str)
        logger.info("✓ Connected to DB2")
        
        logger.info("Executing test query...")
        cursor = conn.cursor()
        cursor.execute(test_query)
        logger.info("✓ Query executed")
        
        logger.info("Fetching results...")
        records = cursor.fetchall()
        logger.info(f"✓ Fetched {len(records)} records")
        
        if records:
            logger.info("\nSample record:")
            record = records[0]
            logger.info(f"  Loan Number: {record[1]}")
            logger.info(f"  Transaction Date: {record[2]}")
            logger.info(f"  Currency: {record[3]}")
            logger.info(f"  Amount: {record[4]}")
        else:
            logger.warning("No records found!")
        
        cursor.close()
        conn.close()
        logger.info("✓ Test completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    test_query()
