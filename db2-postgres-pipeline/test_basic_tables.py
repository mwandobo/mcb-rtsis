#!/usr/bin/env python3
"""
Test Basic Tables for Investment Securities
"""

import pyodbc
import logging
from config import Config

def test_basic_tables():
    """Test basic table access and counts"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing Basic Tables for Investment Securities")
    logger.info("=" * 50)
    
    try:
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
        )
        
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        
        # Test basic table counts
        tables_to_test = [
            ("GLI_TRX_EXTRACT", "SELECT COUNT(*) FROM GLI_TRX_EXTRACT"),
            ("GLG_ACCOUNT", "SELECT COUNT(*) FROM GLG_ACCOUNT"),
            ("DEPOSIT_ACCOUNT", "SELECT COUNT(*) FROM DEPOSIT_ACCOUNT"),
            ("CUSTOMER", "SELECT COUNT(*) FROM CUSTOMER"),
            ("CURRENCY", "SELECT COUNT(*) FROM CURRENCY"),
        ]
        
        for table_name, query in tables_to_test:
            try:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logger.info(f"{table_name:<20}: {count:>15,} records")
            except Exception as e:
                logger.error(f"{table_name:<20}: ERROR - {str(e)[:50]}...")
        
        # Test GLI_TRX_EXTRACT with 130% GL accounts
        logger.info("\n🔍 Testing GLI_TRX_EXTRACT with 130% GL accounts...")
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM GLI_TRX_EXTRACT gte
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                WHERE gl.EXTERNAL_GLACCOUNT LIKE '130%'
            """)
            count = cursor.fetchone()[0]
            logger.info(f"GLI_TRX_EXTRACT with 130% GL accounts: {count:,} records")
        except Exception as e:
            logger.error(f"GLI_TRX_EXTRACT 130% test failed: {e}")
        
        # Test DEPOSIT_ACCOUNT with investment types
        logger.info("\n🔍 Testing DEPOSIT_ACCOUNT with investment types...")
        try:
            cursor.execute("""
                SELECT DEPOSIT_TYPE, COUNT(*) 
                FROM DEPOSIT_ACCOUNT 
                WHERE DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
                    AND ENTRY_STATUS IN ('1', '6')
                GROUP BY DEPOSIT_TYPE 
                ORDER BY DEPOSIT_TYPE
            """)
            results = cursor.fetchall()
            logger.info("DEPOSIT_ACCOUNT investment types:")
            for deposit_type, count in results:
                logger.info(f"  Type {deposit_type}: {count:>10,} records")
        except Exception as e:
            logger.error(f"DEPOSIT_ACCOUNT investment types test failed: {e}")
        
        # Test a very simple query from each table
        logger.info("\n🔍 Testing simple queries...")
        
        simple_queries = [
            ("GLI_TRX_EXTRACT sample", "SELECT FK_GLG_ACCOUNTACCO, DC_AMOUNT FROM GLI_TRX_EXTRACT WHERE DC_AMOUNT > 0 FETCH FIRST 1 ROWS ONLY"),
            ("DEPOSIT_ACCOUNT sample", "SELECT ACCOUNT_NUMBER, DEPOSIT_TYPE, BOOK_BALANCE FROM DEPOSIT_ACCOUNT WHERE DEPOSIT_TYPE IN ('1','2','3','4','5') FETCH FIRST 1 ROWS ONLY"),
        ]
        
        for desc, query in simple_queries:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                logger.info(f"{desc}: {result}")
            except Exception as e:
                logger.error(f"{desc} failed: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_basic_tables()