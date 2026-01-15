#!/usr/bin/env python3
"""
Test the exact SQL from the sqls/balances-with-mnos.sql file
"""

import logging
from db2_connection import DB2Connection

def test_exact_sql_file():
    """Test the exact SQL from the file"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 TESTING EXACT SQL FROM FILE")
    logger.info("=" * 50)
    
    try:
        # Read the exact SQL from file
        with open('../sqls/balances-with-mnos.sql', 'r') as f:
            sql_query = f.read().strip()
        
        # Remove the trailing semicolon if present
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]
        
        logger.info("📋 SQL from file:")
        logger.info(sql_query)
        logger.info("=" * 50)
        
        # Initialize DB2 connection
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Add FETCH FIRST to limit results
            test_query = sql_query + " FETCH FIRST 5 ROWS ONLY"
            cursor.execute(test_query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"📊 Columns: {columns}")
            logger.info("=" * 50)
            
            logger.info("📋 Sample Data from exact SQL file:")
            row_count = 0
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                
                row_count += 1
                logger.info(f"Row {row_count}:")
                for i, (col, val) in enumerate(zip(columns, row)):
                    if col == 'TILLNUMBER':
                        logger.info(f"  {i}: {col} = '{val}' (type: {type(val)}) <<<< TILL NUMBER")
                    else:
                        logger.info(f"  {i}: {col} = {val} (type: {type(val)})")
                logger.info("-" * 30)
                
                if row_count >= 5:
                    break
            
            logger.info(f"✅ Found {row_count} sample records from exact SQL file")
        
    except Exception as e:
        logger.error(f"❌ Error testing exact SQL file: {e}")
        raise

if __name__ == "__main__":
    test_exact_sql_file()