#!/usr/bin/env python3
"""
Count customers by CUST_TYPE
"""

import sys
import os
import logging

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db2_connection import DB2Connection

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def count_by_cust_type():
    """Count customers by CUST_TYPE"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 50)
            logger.info("CUSTOMER COUNT BY CUST_TYPE")
            logger.info("=" * 50)
            
            # Count by CUST_TYPE
            query = """
                SELECT 
                    CUST_TYPE,
                    COUNT(*) as count
                FROM CUSTOMER
                GROUP BY CUST_TYPE
                ORDER BY CUST_TYPE
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            total = 0
            for cust_type, count in results:
                logger.info(f"CUST_TYPE '{cust_type}': {count:,}")
                total += count
            
            logger.info("-" * 30)
            logger.info(f"TOTAL: {total:,}")
            logger.info("=" * 50)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    count_by_cust_type()