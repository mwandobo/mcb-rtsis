#!/usr/bin/env python3
"""
List all columns in CUSTOMER table
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_customer_columns():
    """List all columns in CUSTOMER table"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("CUSTOMER TABLE COLUMNS")
            logger.info("=" * 80)
            
            # Get column information
            query = """
                SELECT 
                    COLNAME,
                    TYPENAME,
                    LENGTH,
                    NULLS
                FROM SYSCAT.COLUMNS
                WHERE TABSCHEMA = 'PROFITS'
                    AND TABNAME = 'CUSTOMER'
                ORDER BY COLNO
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"\nTotal columns: {len(results)}\n")
            logger.info(f"{'Column Name':<40} {'Type':<15} {'Length':<10} {'Nullable'}")
            logger.info("-" * 80)
            
            for col_name, type_name, length, nulls in results:
                nullable = "YES" if nulls == 'Y' else "NO"
                logger.info(f"{col_name:<40} {type_name:<15} {length:<10} {nullable}")
            
            logger.info("\n" + "=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    list_customer_columns()
