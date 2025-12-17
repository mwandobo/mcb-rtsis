#!/usr/bin/env python3
"""
Check if overdraft table exists
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_tables():
    """Check available tables"""
    logger.info("🧪 Checking Available Tables")
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if W_EOM_LOAN_ACCOUNT exists
            table_check_query = """
            SELECT COUNT(*) 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = 'PROFITS' 
            AND TABNAME LIKE '%LOAN%'
            """
            
            logger.info("📊 Checking for loan-related tables...")
            cursor.execute(table_check_query)
            count = cursor.fetchone()[0]
            
            logger.info(f"✅ Found {count} loan-related tables")
            
            # List loan tables
            list_query = """
            SELECT TABNAME 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = 'PROFITS' 
            AND TABNAME LIKE '%LOAN%'
            FETCH FIRST 10 ROWS ONLY
            """
            
            cursor.execute(list_query)
            tables = cursor.fetchall()
            
            logger.info("📋 Available loan tables:")
            for table in tables:
                logger.info(f"  - {table[0]}")
            
            # Check W_EOM_LOAN_ACCOUNT specifically
            specific_check = """
            SELECT COUNT(*) 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = 'PROFITS' 
            AND TABNAME = 'W_EOM_LOAN_ACCOUNT'
            """
            
            cursor.execute(specific_check)
            exists = cursor.fetchone()[0]
            
            if exists:
                logger.info("✅ W_EOM_LOAN_ACCOUNT table exists")
                
                # Check columns
                column_query = """
                SELECT COLNAME 
                FROM SYSCAT.COLUMNS 
                WHERE TABSCHEMA = 'PROFITS' 
                AND TABNAME = 'W_EOM_LOAN_ACCOUNT'
                AND COLNAME LIKE '%OVERDRAFT%'
                """
                
                cursor.execute(column_query)
                columns = cursor.fetchall()
                
                logger.info("📋 Overdraft-related columns:")
                for col in columns:
                    logger.info(f"  - {col[0]}")
            else:
                logger.warning("⚠️ W_EOM_LOAN_ACCOUNT table does not exist")
            
    except Exception as e:
        logger.error(f"❌ Table check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_tables()