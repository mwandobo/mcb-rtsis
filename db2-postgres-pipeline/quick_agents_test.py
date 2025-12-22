#!/usr/bin/env python3
"""
Quick test of enhanced agents query
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def quick_agents_test():
    """Quick test of enhanced agents query"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("Testing enhanced agents query...")
            
            # Test count query
            count_query = """
                SELECT COUNT(*) as total_agents
                FROM CUSTOMER c
                LEFT JOIN AGENT_TERMINAL at ON at.FK_AGENT_CUST_ID = c.CUST_ID AND at.ENTRY_STATUS = '1'
                WHERE c.CUST_ID IN (186,8536,8661,9368,13692,16765,22410,23958,25980,26587,26962,28651,32799,32992,34671,34967,37538,38208,38480,38971,38988,39122,39572,40248,41480,42338,42488,43415,45012,45117,45186,47027,47054,47283,48297,48877,50489,51611,51853,51893,52592,52733,52815,55606,56431,57175,59921,60087,60130,60175,60265,60611,60723,61305,61335,61927,62098,62310,62673)
                    AND COALESCE(c.LAST_UPDATE, c.CUSTOMER_BEGIN_DAT) >= TIMESTAMP('2016-01-01 00:00:00')
            """
            cursor.execute(count_query)
            total_count = cursor.fetchone()[0]
            
            logger.info(f"✅ Enhanced agents query works! Found {total_count} agents")
            
            # Test sample query
            sample_query = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) AS agentName,
                    COALESCE(at.USER_CODE, 'NO_TERMINAL') AS tillNumber,
                    COALESCE(at.LOCATION, 'NO_LOCATION') AS location
                FROM CUSTOMER c
                LEFT JOIN AGENT_TERMINAL at ON at.FK_AGENT_CUST_ID = c.CUST_ID AND at.ENTRY_STATUS = '1'
                WHERE c.CUST_ID IN (186,8536,8661,9368,13692,16765,22410,23958,25980,26587,26962,28651,32799,32992,34671,34967,37538,38208,38480,38971,38988,39122,39572,40248,41480,42338,42488,43415,45012,45117,45186,47027,47054,47283,48297,48877,50489,51611,51853,51893,52592,52733,52815,55606,56431,57175,59921,60087,60130,60175,60265,60611,60723,61305,61335,61927,62098,62310,62673)
                    AND COALESCE(c.LAST_UPDATE, c.CUSTOMER_BEGIN_DAT) >= TIMESTAMP('2016-01-01 00:00:00')
                ORDER BY c.CUST_ID
                FETCH FIRST 5 ROWS ONLY
            """
            cursor.execute(sample_query)
            sample_results = cursor.fetchall()
            
            logger.info("Sample agent records:")
            for cust_id, agent_name, till_number, location in sample_results:
                logger.info(f"  {cust_id}: {agent_name} | Till: {till_number} | Location: {location[:50]}")
            
            logger.info("✅ Enhanced agents query is ready for pipeline!")
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    quick_agents_test()