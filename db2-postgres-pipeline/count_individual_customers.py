#!/usr/bin/env python3
"""
Count individual customers (CUST_TYPE = '1') in DB2
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

def count_individual_customers():
    """Count individual customers with various filters"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            # Total individual customers
            logger.info("=" * 70)
            logger.info("INDIVIDUAL CUSTOMERS (CUST_TYPE = '1') ANALYSIS")
            logger.info("=" * 70)
            
            # 1. Total count
            query1 = """
                SELECT COUNT(*) as total_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '1'
            """
            cursor.execute(query1)
            total = cursor.fetchone()[0]
            logger.info(f"\n📊 Total Individual Customers: {total:,}")
            
            # 2. Active individual customers
            query2 = """
                SELECT COUNT(*) as active_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '1'
                    AND ENTRY_STATUS = '1'
            """
            cursor.execute(query2)
            active = cursor.fetchone()[0]
            logger.info(f"✅ Active Individual Customers: {active:,}")
            
            # 3. Individual customers with mobile numbers
            query3 = """
                SELECT COUNT(*) as mobile_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '1'
                    AND ENTRY_STATUS = '1'
                    AND MOBILE_TEL IS NOT NULL
                    AND MOBILE_TEL != ''
                    AND LENGTH(TRIM(MOBILE_TEL)) > 5
            """
            cursor.execute(query3)
            with_mobile = cursor.fetchone()[0]
            logger.info(f"📱 Active with Valid Mobile Numbers: {with_mobile:,}")
            
            # 4. Individual customers with agent-like names
            query4 = """
                SELECT COUNT(*) as agent_like_count
                FROM CUSTOMER
                WHERE CUST_TYPE = '1'
                    AND ENTRY_STATUS = '1'
                    AND MOBILE_TEL IS NOT NULL
                    AND MOBILE_TEL != ''
                    AND LENGTH(TRIM(MOBILE_TEL)) > 5
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
            logger.info(f"🏪 Agent-like Names (WAKALA/DUKA/SHOP): {agent_like:,}")
            
            # 5. Show sample agent-like customers
            query5 = """
                SELECT 
                    CUST_ID,
                    TRIM(COALESCE(FIRST_NAME, '') || ' ' || COALESCE(MIDDLE_NAME, '') || ' ' || COALESCE(SURNAME, '')) AS full_name,
                    MOBILE_TEL,
                    CUSTOMER_BEGIN_DAT
                FROM CUSTOMER
                WHERE CUST_TYPE = '1'
                    AND ENTRY_STATUS = '1'
                    AND MOBILE_TEL IS NOT NULL
                    AND MOBILE_TEL != ''
                    AND LENGTH(TRIM(MOBILE_TEL)) > 5
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
                ORDER BY CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query5)
            samples = cursor.fetchall()
            
            logger.info(f"\n📋 Sample Agent-like Individual Customers (showing {len(samples)}):")
            logger.info("-" * 70)
            for row in samples:
                cust_id, name, mobile, begin_date = row
                logger.info(f"  ID: {cust_id:6} | {name:40} | {mobile:15} | {begin_date}")
            
            # 6. Breakdown by name pattern
            logger.info("\n📊 Breakdown by Name Pattern:")
            logger.info("-" * 70)
            
            patterns = [
                ('WAKALA', 'Agent in Swahili'),
                ('DUKA', 'Shop in Swahili'),
                ('MADUKA', 'Shops in Swahili'),
                ('SHOP', 'Shop in English'),
                ('STORE', 'Store in English'),
                ('AGENT', 'Agent in English'),
                ('MTEJA', 'Customer in Swahili')
            ]
            
            for pattern, description in patterns:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM CUSTOMER
                    WHERE CUST_TYPE = '1'
                        AND ENTRY_STATUS = '1'
                        AND MOBILE_TEL IS NOT NULL
                        AND MOBILE_TEL != ''
                        AND LENGTH(TRIM(MOBILE_TEL)) > 5
                        AND (UPPER(FIRST_NAME) LIKE '%{pattern}%'
                             OR UPPER(SURNAME) LIKE '%{pattern}%')
                """
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logger.info(f"  {pattern:10} ({description:25}): {count:3}")
            
            # Summary
            logger.info("\n" + "=" * 70)
            logger.info("📊 SUMMARY:")
            logger.info(f"  Total Individual Customers: {total:,}")
            logger.info(f"  Active: {active:,} ({active/total*100:.1f}%)")
            logger.info(f"  With Mobile: {with_mobile:,} ({with_mobile/active*100:.1f}% of active)")
            logger.info(f"  Agent-like: {agent_like:,} ({agent_like/with_mobile*100:.2f}% of mobile users)")
            logger.info("=" * 70)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    count_individual_customers()
