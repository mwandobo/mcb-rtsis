#!/usr/bin/env python3
"""
Check CUST_TYPE '2' customers to see if they might be agents
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

def check_cust_type_2():
    """Check CUST_TYPE '2' customers for potential agents"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("CUST_TYPE '2' (CORPORATE/BUSINESS) CUSTOMERS ANALYSIS")
            logger.info("=" * 80)
            
            # 1. Total count
            query1 = """
                SELECT COUNT(*) as total_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '2'
            """
            cursor.execute(query1)
            total = cursor.fetchone()[0]
            logger.info(f"\n📊 Total CUST_TYPE '2' Customers: {total:,}")
            
            # 2. Active customers
            query2 = """
                SELECT COUNT(*) as active_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '2'
                    AND ENTRY_STATUS = '1'
            """
            cursor.execute(query2)
            active = cursor.fetchone()[0]
            logger.info(f"✅ Active CUST_TYPE '2' Customers: {active:,}")
            
            # 3. With mobile numbers
            query3 = """
                SELECT COUNT(*) as mobile_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '2'
                    AND ENTRY_STATUS = '1'
                    AND MOBILE_TEL IS NOT NULL
                    AND MOBILE_TEL != ''
                    AND LENGTH(TRIM(MOBILE_TEL)) > 5
            """
            cursor.execute(query3)
            with_mobile = cursor.fetchone()[0]
            logger.info(f"📱 Active with Valid Mobile Numbers: {with_mobile:,}")
            
            # 4. Look for agent-like names
            query4 = """
                SELECT COUNT(*) as agent_like_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '2'
                    AND ENTRY_STATUS = '1'
                    AND (UPPER(FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(SURNAME) LIKE '%AGENT%'
                         OR UPPER(FIRST_NAME) LIKE '%WAKALA%'
                         OR UPPER(SURNAME) LIKE '%WAKALA%'
                         OR UPPER(FIRST_NAME) LIKE '%DUKA%'
                         OR UPPER(SURNAME) LIKE '%DUKA%'
                         OR UPPER(FIRST_NAME) LIKE '%SHOP%'
                         OR UPPER(SURNAME) LIKE '%SHOP%'
                         OR UPPER(FIRST_NAME) LIKE '%STORE%'
                         OR UPPER(SURNAME) LIKE '%STORE%'
                         OR UPPER(FIRST_NAME) LIKE '%MTEJA%'
                         OR UPPER(SURNAME) LIKE '%MTEJA%')
            """
            cursor.execute(query4)
            agent_like = cursor.fetchone()[0]
            logger.info(f"🏪 Agent-like Names: {agent_like:,}")
            
            # 5. Show all CUST_TYPE '2' customers (since there are only 736)
            query5 = """
                SELECT 
                    CUST_ID,
                    TRIM(COALESCE(FIRST_NAME, '') || ' ' || COALESCE(MIDDLE_NAME, '') || ' ' || COALESCE(SURNAME, '')) AS full_name,
                    MOBILE_TEL,
                    ENTRY_STATUS,
                    CUSTOMER_BEGIN_DAT
                FROM CUSTOMER
                WHERE CUST_TYPE = '2'
                ORDER BY ENTRY_STATUS DESC, CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 50 ROWS ONLY
            """
            cursor.execute(query5)
            samples = cursor.fetchall()
            
            logger.info(f"\n📋 Sample CUST_TYPE '2' Customers (showing first 50 of {total}):")
            logger.info("-" * 80)
            logger.info("  ID     | Name                                     | Mobile          | Status | Date")
            logger.info("-" * 80)
            
            for row in samples:
                cust_id, name, mobile, status, begin_date = row
                status_text = "Active" if status == '1' else "Inactive"
                mobile_display = mobile if mobile else "No Mobile"
                logger.info(f"  {cust_id:6} | {name:40} | {mobile_display:15} | {status_text:6} | {begin_date}")
            
            # 6. Look for specific agent-related keywords in names
            logger.info(f"\n🔍 Searching for Agent-related Keywords in CUST_TYPE '2':")
            logger.info("-" * 80)
            
            keywords = [
                ('AGENT', 'Agent'),
                ('WAKALA', 'Agent in Swahili'),
                ('DUKA', 'Shop in Swahili'),
                ('MADUKA', 'Shops in Swahili'),
                ('SHOP', 'Shop'),
                ('STORE', 'Store'),
                ('MOBILE', 'Mobile Money'),
                ('MONEY', 'Money Services'),
                ('MPESA', 'M-Pesa'),
                ('TIGO', 'Tigo Pesa'),
                ('AIRTEL', 'Airtel Money'),
                ('HALO', 'Halo Pesa'),
                ('FINANCIAL', 'Financial Services'),
                ('SERVICE', 'Services'),
                ('BUSINESS', 'Business'),
                ('COMPANY', 'Company'),
                ('LIMITED', 'Limited Company'),
                ('LTD', 'Limited'),
                ('COOPERATIVE', 'Cooperative'),
                ('SACCO', 'SACCO')
            ]
            
            for keyword, description in keywords:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM CUSTOMER
                    WHERE CUST_TYPE = '2'
                        AND ENTRY_STATUS = '1'
                        AND (UPPER(FIRST_NAME) LIKE '%{keyword}%'
                             OR UPPER(SURNAME) LIKE '%{keyword}%')
                """
                cursor.execute(query)
                count = cursor.fetchone()[0]
                if count > 0:
                    logger.info(f"  {keyword:12} ({description:20}): {count:3}")
            
            # 7. Show customers with mobile money related names
            query7 = """
                SELECT 
                    CUST_ID,
                    TRIM(COALESCE(FIRST_NAME, '') || ' ' || COALESCE(MIDDLE_NAME, '') || ' ' || COALESCE(SURNAME, '')) AS full_name,
                    MOBILE_TEL,
                    CUSTOMER_BEGIN_DAT
                FROM CUSTOMER
                WHERE CUST_TYPE = '2'
                    AND ENTRY_STATUS = '1'
                    AND (UPPER(FIRST_NAME) LIKE '%MOBILE%'
                         OR UPPER(SURNAME) LIKE '%MOBILE%'
                         OR UPPER(FIRST_NAME) LIKE '%MONEY%'
                         OR UPPER(SURNAME) LIKE '%MONEY%'
                         OR UPPER(FIRST_NAME) LIKE '%MPESA%'
                         OR UPPER(SURNAME) LIKE '%MPESA%'
                         OR UPPER(FIRST_NAME) LIKE '%TIGO%'
                         OR UPPER(SURNAME) LIKE '%TIGO%'
                         OR UPPER(FIRST_NAME) LIKE '%AIRTEL%'
                         OR UPPER(SURNAME) LIKE '%AIRTEL%'
                         OR UPPER(FIRST_NAME) LIKE '%HALO%'
                         OR UPPER(SURNAME) LIKE '%HALO%'
                         OR UPPER(FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(SURNAME) LIKE '%AGENT%'
                         OR UPPER(FIRST_NAME) LIKE '%WAKALA%'
                         OR UPPER(SURNAME) LIKE '%WAKALA%'
                         OR UPPER(FIRST_NAME) LIKE '%DUKA%'
                         OR UPPER(SURNAME) LIKE '%DUKA%')
                ORDER BY CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query7)
            mobile_money = cursor.fetchall()
            
            if mobile_money:
                logger.info(f"\n📱 Mobile Money/Agent Related CUST_TYPE '2' Customers:")
                logger.info("-" * 80)
                for row in mobile_money:
                    cust_id, name, mobile, begin_date = row
                    mobile_display = mobile if mobile else "No Mobile"
                    logger.info(f"  ID: {cust_id:6} | {name:50} | {mobile_display:15} | {begin_date}")
            
            # Summary
            logger.info("\n" + "=" * 80)
            logger.info("📊 SUMMARY:")
            logger.info(f"  Total CUST_TYPE '2': {total:,}")
            logger.info(f"  Active: {active:,} ({active/total*100:.1f}%)")
            logger.info(f"  With Mobile: {with_mobile:,} ({with_mobile/active*100:.1f}% of active)")
            logger.info(f"  Agent-like Names: {agent_like:,}")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_cust_type_2()