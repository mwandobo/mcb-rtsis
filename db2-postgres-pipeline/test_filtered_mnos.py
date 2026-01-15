#!/usr/bin/env python3
"""
Test the filtered MNOs query (without DUMMY records)
"""

import logging
from config import Config
from db2_connection import DB2Connection

def test_filtered_mnos():
    """Test the filtered MNOs query"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 TESTING FILTERED MNOS QUERY (NO DUMMY)")
    logger.info("=" * 60)
    
    try:
        # Initialize DB2 connection
        db2_conn = DB2Connection()
        
        # Get table configuration
        table_config = config.tables['balanceWithMnos']
        
        logger.info("📋 Updated Query:")
        logger.info(table_config.query)
        logger.info("=" * 60)
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(table_config.query + " FETCH FIRST 10 ROWS ONLY")
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"📊 Columns: {columns}")
            logger.info("=" * 60)
            
            logger.info("📋 Sample Data (should have NO DUMMY records):")
            row_count = 0
            dummy_count = 0
            
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                
                row_count += 1
                till_number = str(row[3]).strip()
                
                # Check for DUMMY
                if 'DUMMY' in till_number.upper():
                    dummy_count += 1
                    logger.warning(f"⚠️ DUMMY FOUND: {till_number}")
                
                logger.info(f"Row {row_count}:")
                logger.info(f"  - MNO: {row[2]}")
                logger.info(f"  - Till Number: '{till_number}' <<<< SHOULD NOT BE DUMMY")
                logger.info(f"  - Currency: {row[4]}")
                logger.info(f"  - Amount: {row[7]}")
                logger.info("-" * 30)
                
                if row_count >= 10:
                    break
            
            logger.info("=" * 60)
            logger.info(f"📊 SUMMARY:")
            logger.info(f"   - Total records: {row_count}")
            logger.info(f"   - DUMMY records found: {dummy_count}")
            
            if dummy_count == 0:
                logger.info("✅ SUCCESS: No DUMMY records found!")
            else:
                logger.warning(f"⚠️ WARNING: {dummy_count} DUMMY records still found")
        
    except Exception as e:
        logger.error(f"❌ Error testing filtered query: {e}")
        raise

if __name__ == "__main__":
    test_filtered_mnos()