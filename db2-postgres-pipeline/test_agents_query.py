#!/usr/bin/env python3
"""
Test Agents Query
"""

import logging
from db2_connection import DB2Connection

def test_agents_query():
    """Test the agents query"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 TESTING AGENTS QUERY")
    logger.info("=" * 50)
    
    try:
        # Initialize DB2 connection
        db2_conn = DB2Connection()
        
        # Test simple query first
        simple_query = """
        SELECT COUNT(*) 
        FROM AGENTS_LIST al
        LEFT JOIN BANKEMPLOYEE be ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(be.STAFF_NO)
        """
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            logger.info("📊 Testing simple count query...")
            cursor.execute(simple_query)
            count = cursor.fetchone()[0]
            logger.info(f"Total records in AGENTS_LIST with BANKEMPLOYEE join: {count}")
            
            # Test AGENTS_LIST table
            logger.info("📊 Testing AGENTS_LIST table...")
            cursor.execute("SELECT COUNT(*) FROM AGENTS_LIST")
            agents_count = cursor.fetchone()[0]
            logger.info(f"Total records in AGENTS_LIST: {agents_count}")
            
            # Test BANKEMPLOYEE table
            logger.info("📊 Testing BANKEMPLOYEE table...")
            cursor.execute("SELECT COUNT(*) FROM BANKEMPLOYEE")
            employee_count = cursor.fetchone()[0]
            logger.info(f"Total records in BANKEMPLOYEE: {employee_count}")
            
            # Test sample from AGENTS_LIST
            logger.info("📋 Sample from AGENTS_LIST:")
            cursor.execute("SELECT AGENT_ID, TERMINAL_ID, BUSINESS_FORM FROM AGENTS_LIST FETCH FIRST 3 ROWS ONLY")
            for row in cursor.fetchall():
                logger.info(f"  - Agent ID: {row[0]}, Terminal ID: {row[1]}, Business Form: {row[2]}")
            
            # Test sample from BANKEMPLOYEE
            logger.info("📋 Sample from BANKEMPLOYEE:")
            cursor.execute("SELECT STAFF_NO, FIRST_NAME, LAST_NAME FROM BANKEMPLOYEE FETCH FIRST 3 ROWS ONLY")
            for row in cursor.fetchall():
                logger.info(f"  - Staff No: {row[0]}, Name: {row[1]} {row[2]}")
        
        logger.info("✅ Test completed!")
        
    except Exception as e:
        logger.error(f"❌ Error in test: {e}")
        raise

if __name__ == "__main__":
    test_agents_query()