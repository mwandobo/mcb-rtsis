#!/usr/bin/env python3
"""
Check customers with BUSINESS_IND = '1' as potential agents
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_business_customers():
    """Check customers with BUSINESS_IND = '1'"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("CUSTOMERS WITH BUSINESS_IND = '1' (POTENTIAL AGENTS)")
            logger.info("=" * 80)
            
            # Get all customers with BUSINESS_IND = '1'
            query = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.MOBILE_TEL,
                    c.EMPLOYER,
                    c.ENTRY_STATUS,
                    c.CUSTOMER_BEGIN_DAT,
                    c.VIP_IND,
                    c.CUST_STATUS,
                    c.DAILY_ORDER_AMNT
                FROM CUSTOMER c
                WHERE c.BUSINESS_IND = '1'
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"\n📊 Found {len(results)} customers with BUSINESS_IND = '1'")
            logger.info("-" * 80)
            logger.info("ID     | Name                           | Type | Mobile          | Status | VIP | Date       | Employer")
            logger.info("-" * 80)
            
            active_with_mobile = 0
            for row in results:
                cust_id, name, cust_type, mobile, employer, entry_status, begin_date, vip_ind, cust_status, daily_amt = row
                
                status_text = "Active" if entry_status == '1' else "Inactive"
                mobile_display = mobile if mobile else "No Mobile"
                employer_display = employer[:20] if employer else "No Employer"
                vip_display = "VIP" if vip_ind == '1' else ""
                
                if entry_status == '1' and mobile:
                    active_with_mobile += 1
                
                logger.info(f"{cust_id:6} | {name:30} | {cust_type:4} | {mobile_display:15} | {status_text:6} | {vip_display:3} | {begin_date} | {employer_display}")
            
            logger.info("-" * 80)
            logger.info(f"📱 Active customers with mobile numbers: {active_with_mobile}")
            
            # Check if any have agent-like names or employers
            logger.info("\n🔍 Checking for agent-related terms in names/employers:")
            agent_query = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.MOBILE_TEL,
                    c.EMPLOYER
                FROM CUSTOMER c
                WHERE c.BUSINESS_IND = '1'
                    AND c.ENTRY_STATUS = '1'
                    AND (UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(c.SURNAME) LIKE '%AGENT%'
                         OR UPPER(c.EMPLOYER) LIKE '%AGENT%'
                         OR UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                         OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                         OR UPPER(c.EMPLOYER) LIKE '%WAKALA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%MOBILE%'
                         OR UPPER(c.SURNAME) LIKE '%MOBILE%'
                         OR UPPER(c.EMPLOYER) LIKE '%MOBILE%'
                         OR UPPER(c.FIRST_NAME) LIKE '%MONEY%'
                         OR UPPER(c.SURNAME) LIKE '%MONEY%'
                         OR UPPER(c.EMPLOYER) LIKE '%MONEY%'
                         OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                         OR UPPER(c.SURNAME) LIKE '%DUKA%'
                         OR UPPER(c.EMPLOYER) LIKE '%DUKA%')
            """
            cursor.execute(agent_query)
            agent_results = cursor.fetchall()
            
            if agent_results:
                logger.info("  Agent-related business customers:")
                for cust_id, name, cust_type, mobile, employer in agent_results:
                    mobile_display = mobile if mobile else "No Mobile"
                    employer_display = employer if employer else "No Employer"
                    logger.info(f"    {cust_id:6}: {name:40} | {mobile_display:15} | {employer_display}")
            else:
                logger.info("  No agent-related terms found in business customers")
            
            # Summary recommendation
            logger.info("\n" + "=" * 80)
            logger.info("📊 ANALYSIS SUMMARY:")
            logger.info(f"  Total BUSINESS_IND = '1': {len(results)}")
            logger.info(f"  Active with mobile: {active_with_mobile}")
            logger.info(f"  Agent-related: {len(agent_results)}")
            
            if active_with_mobile > 0:
                logger.info("\n💡 RECOMMENDATION:")
                logger.info("  BUSINESS_IND = '1' customers could be potential agents")
                logger.info("  These are customers marked as having business activities")
                logger.info("  Consider using these instead of name-based filtering")
            else:
                logger.info("\n❌ BUSINESS_IND = '1' customers don't seem suitable for agents")
            
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_business_customers()