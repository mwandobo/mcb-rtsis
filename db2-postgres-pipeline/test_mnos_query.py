#!/usr/bin/env python3
"""
Test MNOs query to see actual data
"""

import logging
from config import Config
from db2_connection import DB2Connection

def test_mnos_query():
    """Test the MNOs query to see what data we get"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 TESTING MNOS QUERY")
    logger.info("=" * 50)
    
    try:
        # Initialize DB2 connection
        db2_conn = DB2Connection()
        
        # Get table configuration
        table_config = config.tables['balanceWithMnos']
        
        logger.info("📋 Query:")
        logger.info(table_config.query)
        logger.info("=" * 50)
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(table_config.query + " FETCH FIRST 5 ROWS ONLY")
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"📊 Columns: {columns}")
            logger.info("=" * 50)
            
            logger.info("📋 Sample Data:")
            row_count = 0
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                
                row_count += 1
                logger.info(f"Row {row_count}:")
                for i, (col, val) in enumerate(zip(columns, row)):
                    logger.info(f"  {i}: {col} = {val} (type: {type(val)})")
                logger.info("-" * 30)
                
                if row_count >= 5:
                    break
            
            logger.info(f"✅ Found {row_count} sample records")
        
    except Exception as e:
        logger.error(f"❌ Error testing query: {e}")
        raise

if __name__ == "__main__":
    test_mnos_query()