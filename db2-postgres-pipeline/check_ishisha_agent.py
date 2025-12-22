#!/usr/bin/env python3
"""
Check CUST_TYPE of ISHISHA MIN SHOP AGENT
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_ishisha_agent():
    """Check ISHISHA MIN SHOP AGENT customer details"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("SEARCHING FOR ISHISHA MIN SHOP AGENT")
            logger.info("=" * 80)
            
            # Search for ISHISHA customer
            query = """
                SELECT 
                    c.CUST_ID,
                    c.FIRST_NAME,
                    c.MIDDLE_NAME,
                    c.SURNAME,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                    c.CUST_TYPE,
                    c.ENTRY_STATUS,
                    c.MOBILE_TEL,
                    c.BUSINESS_IND,
                    c.VIP_IND,
                    c.CUSTOMER_BEGIN_DAT,
                    c.EMPLOYER
                FROM CUSTOMER c
                WHERE (UPPER(c.FIRST_NAME) LIKE '%ISHISHA%'
                       OR UPPER(c.MIDDLE_NAME) LIKE '%ISHISHA%'
                       OR UPPER(c.SURNAME) LIKE '%ISHISHA%')
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            if results:
                logger.info(f"\n📊 Found {len(results)} customer(s) with 'ISHISHA' in name:")
                logger.info("-" * 80)
                
                for row in results:
                    cust_id, first_name, middle_name, surname, full_name, cust_type, entry_status, mobile_tel, business_ind, vip_ind, begin_date, employer = row
                    
                    status_text = "Active" if entry_status == '1' else "Inactive"
                    mobile_display = mobile_tel if mobile_tel else "No Mobile"
                    employer_display = employer if employer else "No Employer"
                    
                    logger.info(f"Customer ID: {cust_id}")
                    logger.info(f"Full Name: {full_name}")
                    logger.info(f"CUST_TYPE: {cust_type}")
                    logger.info(f"Status: {status_text}")
                    logger.info(f"Mobile: {mobile_display}")
                    logger.info(f"Business Indicator: {business_ind}")
                    logger.info(f"VIP Indicator: {vip_ind}")
                    logger.info(f"Registration Date: {begin_date}")
                    logger.info(f"Employer: {employer_display}")
                    logger.info("-" * 80)
            else:
                logger.info("\n❌ No customer found with 'ISHISHA' in name")
                
                # Try broader search for SHOP AGENT
                logger.info("\n🔍 Searching for customers with 'SHOP AGENT' in name:")
                query2 = """
                    SELECT 
                        c.CUST_ID,
                        TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                        c.CUST_TYPE,
                        c.ENTRY_STATUS,
                        c.MOBILE_TEL
                    FROM CUSTOMER c
                    WHERE (UPPER(c.FIRST_NAME) LIKE '%SHOP%' AND UPPER(c.SURNAME) LIKE '%AGENT%')
                        OR (UPPER(c.FIRST_NAME) LIKE '%AGENT%' AND UPPER(c.SURNAME) LIKE '%SHOP%')
                        OR (UPPER(c.MIDDLE_NAME) LIKE '%SHOP%' AND UPPER(c.SURNAME) LIKE '%AGENT%')
                    ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                    FETCH FIRST 10 ROWS ONLY
                """
                cursor.execute(query2)
                shop_results = cursor.fetchall()
                
                if shop_results:
                    for cust_id, full_name, cust_type, entry_status, mobile_tel in shop_results:
                        status_text = "Active" if entry_status == '1' else "Inactive"
                        mobile_display = mobile_tel if mobile_tel else "No Mobile"
                        logger.info(f"  {cust_id:6}: {full_name:40} | CUST_TYPE: {cust_type} | {status_text} | {mobile_display}")
                else:
                    logger.info("  No customers found with 'SHOP AGENT' pattern")
            
            # Also search for any customer with MIN in name
            logger.info("\n🔍 Searching for customers with 'MIN' in name:")
            query3 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                    c.CUST_TYPE,
                    c.ENTRY_STATUS,
                    c.MOBILE_TEL
                FROM CUSTOMER c
                WHERE UPPER(c.FIRST_NAME) LIKE '%MIN%'
                    OR UPPER(c.MIDDLE_NAME) LIKE '%MIN%'
                    OR UPPER(c.SURNAME) LIKE '%MIN%'
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(query3)
            min_results = cursor.fetchall()
            
            if min_results:
                for cust_id, full_name, cust_type, entry_status, mobile_tel in min_results:
                    status_text = "Active" if entry_status == '1' else "Inactive"
                    mobile_display = mobile_tel if mobile_tel else "No Mobile"
                    logger.info(f"  {cust_id:6}: {full_name:40} | CUST_TYPE: {cust_type} | {status_text} | {mobile_display}")
            else:
                logger.info("  No customers found with 'MIN' in name")
            
            logger.info("\n" + "=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_ishisha_agent()