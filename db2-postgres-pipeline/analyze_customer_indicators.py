#!/usr/bin/env python3
"""
Analyze CUSTOMER table for agent indicators using existing fields
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_customer_indicators():
    """Analyze existing CUSTOMER fields for agent indicators"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("ANALYZING CUSTOMER FIELDS FOR AGENT INDICATORS")
            logger.info("=" * 80)
            
            # 1. Check BUSINESS_IND field
            logger.info("\n🔍 Checking BUSINESS_IND field:")
            query1 = """
                SELECT BUSINESS_IND, COUNT(*) as count
                FROM CUSTOMER
                WHERE BUSINESS_IND IS NOT NULL
                GROUP BY BUSINESS_IND
                ORDER BY count DESC
            """
            cursor.execute(query1)
            results = cursor.fetchall()
            for ind, count in results:
                logger.info(f"  BUSINESS_IND '{ind}': {count:,}")
            
            # 2. Check VIP_IND field
            logger.info("\n🔍 Checking VIP_IND field:")
            query2 = """
                SELECT VIP_IND, COUNT(*) as count
                FROM CUSTOMER
                WHERE VIP_IND IS NOT NULL
                GROUP BY VIP_IND
                ORDER BY count DESC
            """
            cursor.execute(query2)
            results = cursor.fetchall()
            for ind, count in results:
                logger.info(f"  VIP_IND '{ind}': {count:,}")
            
            # 3. Check CUST_STATUS field
            logger.info("\n🔍 Checking CUST_STATUS field:")
            query3 = """
                SELECT CUST_STATUS, COUNT(*) as count
                FROM CUSTOMER
                WHERE CUST_STATUS IS NOT NULL
                GROUP BY CUST_STATUS
                ORDER BY count DESC
            """
            cursor.execute(query3)
            results = cursor.fetchall()
            for status, count in results:
                logger.info(f"  CUST_STATUS '{status}': {count:,}")
            
            # 4. Check SEGM_FLAGS field
            logger.info("\n🔍 Checking SEGM_FLAGS field:")
            query4 = """
                SELECT SEGM_FLAGS, COUNT(*) as count
                FROM CUSTOMER
                WHERE SEGM_FLAGS IS NOT NULL
                    AND TRIM(SEGM_FLAGS) != ''
                GROUP BY SEGM_FLAGS
                ORDER BY count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query4)
            results = cursor.fetchall()
            for flag, count in results:
                logger.info(f"  SEGM_FLAGS '{flag}': {count:,}")
            
            # 5. Check customers with high transaction volumes (potential agents)
            logger.info("\n🔍 Checking customers with multiple accounts (potential agents):")
            query5 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.BUSINESS_IND,
                    c.VIP_IND,
                    c.MOBILE_TEL,
                    COUNT(a.ACCOUNT_ID) as account_count
                FROM CUSTOMER c
                LEFT JOIN ACCOUNT a ON a.FK_CUSTOMERID = c.CUST_ID
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                GROUP BY c.CUST_ID, c.FIRST_NAME, c.SURNAME, c.CUST_TYPE, c.BUSINESS_IND, c.VIP_IND, c.MOBILE_TEL
                HAVING COUNT(a.ACCOUNT_ID) >= 3
                ORDER BY account_count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query5)
            results = cursor.fetchall()
            logger.info("  Customers with 3+ accounts:")
            for cust_id, name, cust_type, bus_ind, vip_ind, mobile, acc_count in results:
                logger.info(f"    {cust_id:6}: {name:40} | Type:{cust_type} | Bus:{bus_ind} | VIP:{vip_ind} | Accounts:{acc_count}")
            
            # 6. Check EMPLOYER field for agent-related employers
            logger.info("\n🔍 Checking EMPLOYER field for agent-related terms:")
            query6 = """
                SELECT DISTINCT EMPLOYER, COUNT(*) as count
                FROM CUSTOMER
                WHERE EMPLOYER IS NOT NULL
                    AND (UPPER(EMPLOYER) LIKE '%AGENT%'
                         OR UPPER(EMPLOYER) LIKE '%WAKALA%'
                         OR UPPER(EMPLOYER) LIKE '%MOBILE%'
                         OR UPPER(EMPLOYER) LIKE '%MONEY%'
                         OR UPPER(EMPLOYER) LIKE '%MPESA%'
                         OR UPPER(EMPLOYER) LIKE '%TIGO%'
                         OR UPPER(EMPLOYER) LIKE '%AIRTEL%'
                         OR UPPER(EMPLOYER) LIKE '%HALO%')
                GROUP BY EMPLOYER
                ORDER BY count DESC
            """
            cursor.execute(query6)
            results = cursor.fetchall()
            if results:
                logger.info("  Agent-related employers:")
                for employer, count in results:
                    logger.info(f"    {employer}: {count}")
            else:
                logger.info("  No agent-related employers found")
            
            # 7. Check customers with specific business indicators
            logger.info("\n🔍 Checking customers with BUSINESS_IND = '1':")
            query7 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.MOBILE_TEL,
                    c.EMPLOYER,
                    c.CUSTOMER_BEGIN_DAT
                FROM CUSTOMER c
                WHERE c.BUSINESS_IND = '1'
                    AND c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query7)
            results = cursor.fetchall()
            logger.info(f"  Found {len(results)} business customers with mobile numbers:")
            for cust_id, name, cust_type, mobile, employer, begin_date in results:
                employer_display = employer[:30] if employer else "No Employer"
                logger.info(f"    {cust_id:6}: {name:30} | Type:{cust_type} | {mobile:15} | {employer_display} | {begin_date}")
            
            # 8. Check for customers with specific patterns in names or employers
            logger.info("\n🔍 Looking for service-related businesses:")
            query8 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.MOBILE_TEL,
                    c.EMPLOYER
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND (UPPER(c.FIRST_NAME) LIKE '%SERVICE%'
                         OR UPPER(c.SURNAME) LIKE '%SERVICE%'
                         OR UPPER(c.EMPLOYER) LIKE '%SERVICE%'
                         OR UPPER(c.FIRST_NAME) LIKE '%BUSINESS%'
                         OR UPPER(c.SURNAME) LIKE '%BUSINESS%'
                         OR UPPER(c.EMPLOYER) LIKE '%BUSINESS%')
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 15 ROWS ONLY
            """
            cursor.execute(query8)
            results = cursor.fetchall()
            if results:
                logger.info("  Service/Business related customers:")
                for cust_id, name, cust_type, mobile, employer in results:
                    employer_display = employer[:30] if employer else "No Employer"
                    logger.info(f"    {cust_id:6}: {name:30} | Type:{cust_type} | {mobile:15} | {employer_display}")
            
            # 9. Check DAILY_ORDER_AMNT for high-volume customers
            logger.info("\n🔍 Checking customers with high DAILY_ORDER_AMNT (potential agents):")
            query9 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.MOBILE_TEL,
                    c.DAILY_ORDER_AMNT
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.DAILY_ORDER_AMNT IS NOT NULL
                    AND c.DAILY_ORDER_AMNT > 0
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                ORDER BY c.DAILY_ORDER_AMNT DESC
                FETCH FIRST 15 ROWS ONLY
            """
            cursor.execute(query9)
            results = cursor.fetchall()
            if results:
                logger.info("  High daily order amount customers:")
                for cust_id, name, cust_type, mobile, daily_amt in results:
                    logger.info(f"    {cust_id:6}: {name:30} | Type:{cust_type} | {mobile:15} | Daily:{daily_amt:,.2f}")
            else:
                logger.info("  No customers with DAILY_ORDER_AMNT found")
            
            logger.info("\n" + "=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_customer_indicators()