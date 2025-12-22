#!/usr/bin/env python3
"""
Investigate CUSTOMER table fields for agent indicators
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def investigate_customer_fields():
    """Investigate CUSTOMER table for agent-related fields"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("INVESTIGATING CUSTOMER TABLE FOR AGENT INDICATORS")
            logger.info("=" * 80)
            
            # 1. Check all distinct values in key fields
            logger.info("\n🔍 Checking CUSTOMER_CATEGORY field:")
            query1 = """
                SELECT CUSTOMER_CATEGORY, COUNT(*) as count
                FROM CUSTOMER
                WHERE CUSTOMER_CATEGORY IS NOT NULL
                GROUP BY CUSTOMER_CATEGORY
                ORDER BY count DESC
            """
            cursor.execute(query1)
            results = cursor.fetchall()
            for cat, count in results:
                logger.info(f"  {cat}: {count:,}")
            
            # 2. Check CUSTOMER_CLASS
            logger.info("\n🔍 Checking CUSTOMER_CLASS field:")
            query2 = """
                SELECT CUSTOMER_CLASS, COUNT(*) as count
                FROM CUSTOMER
                WHERE CUSTOMER_CLASS IS NOT NULL
                GROUP BY CUSTOMER_CLASS
                ORDER BY count DESC
            """
            cursor.execute(query2)
            results = cursor.fetchall()
            for cls, count in results:
                logger.info(f"  {cls}: {count:,}")
            
            # 3. Check CUSTOMER_SEGMENT
            logger.info("\n🔍 Checking CUSTOMER_SEGMENT field:")
            query3 = """
                SELECT CUSTOMER_SEGMENT, COUNT(*) as count
                FROM CUSTOMER
                WHERE CUSTOMER_SEGMENT IS NOT NULL
                GROUP BY CUSTOMER_SEGMENT
                ORDER BY count DESC
            """
            cursor.execute(query3)
            results = cursor.fetchall()
            for seg, count in results:
                logger.info(f"  {seg}: {count:,}")
            
            # 4. Check if there's a relationship table
            logger.info("\n🔍 Checking for ACCOUNT table with agent indicators:")
            query4 = """
                SELECT COUNT(*) as count
                FROM ACCOUNT
                WHERE ACCOUNT_TYPE IS NOT NULL
                FETCH FIRST 1 ROWS ONLY
            """
            try:
                cursor.execute(query4)
                logger.info("  ✅ ACCOUNT table exists")
                
                # Check account types
                query4b = """
                    SELECT ACCOUNT_TYPE, COUNT(*) as count
                    FROM ACCOUNT
                    GROUP BY ACCOUNT_TYPE
                    ORDER BY count DESC
                    FETCH FIRST 20 ROWS ONLY
                """
                cursor.execute(query4b)
                results = cursor.fetchall()
                logger.info("\n  Account Types:")
                for acc_type, count in results:
                    logger.info(f"    {acc_type}: {count:,}")
            except Exception as e:
                logger.info(f"  ❌ ACCOUNT table check failed: {e}")
            
            # 5. Check GENERIC_DETAIL for agent-related codes
            logger.info("\n🔍 Checking GENERIC_DETAIL for agent-related codes:")
            query5 = """
                SELECT FK_GENERIC_HEADPAR, SERIAL_NUM, DESCRIPTION, LATIN_DESC
                FROM GENERIC_DETAIL
                WHERE (UPPER(DESCRIPTION) LIKE '%AGENT%'
                       OR UPPER(LATIN_DESC) LIKE '%AGENT%'
                       OR UPPER(DESCRIPTION) LIKE '%WAKALA%'
                       OR UPPER(LATIN_DESC) LIKE '%WAKALA%'
                       OR UPPER(DESCRIPTION) LIKE '%MOBILE%'
                       OR UPPER(LATIN_DESC) LIKE '%MOBILE%')
                    AND ENTRY_STATUS = '1'
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query5)
            results = cursor.fetchall()
            if results:
                logger.info("  Found agent-related codes:")
                for head, serial, desc, latin in results:
                    logger.info(f"    {head}-{serial}: {desc} / {latin}")
            else:
                logger.info("  No agent-related codes found")
            
            # 6. Check if customers have links to GENERIC_DETAIL
            logger.info("\n🔍 Checking CUSTOMER fields that link to GENERIC_DETAIL:")
            query6 = """
                SELECT 
                    COUNT(DISTINCT FKGH_HAS_CUST_CATE) as cust_category_codes,
                    COUNT(DISTINCT FKGH_HAS_CUST_CLAS) as cust_class_codes,
                    COUNT(DISTINCT FKGH_HAS_CUST_SEGM) as cust_segment_codes
                FROM CUSTOMER
            """
            cursor.execute(query6)
            result = cursor.fetchone()
            logger.info(f"  Customer Category Codes: {result[0]}")
            logger.info(f"  Customer Class Codes: {result[1]}")
            logger.info(f"  Customer Segment Codes: {result[2]}")
            
            # 7. Sample customers with different categories
            logger.info("\n🔍 Sample customers with CUSTOMER_CATEGORY:")
            query7 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.CUSTOMER_CATEGORY,
                    gd.DESCRIPTION as category_desc,
                    gd.LATIN_DESC as category_latin
                FROM CUSTOMER c
                LEFT JOIN GENERIC_DETAIL gd ON gd.FK_GENERIC_HEADPAR = c.FKGH_HAS_CUST_CATE
                    AND gd.SERIAL_NUM = c.FKGD_HAS_CUST_CATE
                    AND gd.ENTRY_STATUS = '1'
                WHERE c.CUSTOMER_CATEGORY IS NOT NULL
                    AND c.ENTRY_STATUS = '1'
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query7)
            results = cursor.fetchall()
            for cust_id, name, cust_type, cat, desc, latin in results:
                logger.info(f"  {cust_id}: {name:30} | Type:{cust_type} | Cat:{cat} | {desc} / {latin}")
            
            # 8. Check for transaction patterns that might indicate agents
            logger.info("\n🔍 Checking for high-transaction customers (potential agents):")
            query8 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    COUNT(*) as txn_count
                FROM CUSTOMER c
                JOIN ACCOUNT a ON a.FK_CUSTOMERID = c.CUST_ID
                WHERE c.ENTRY_STATUS = '1'
                    AND a.ENTRY_STATUS = '1'
                GROUP BY c.CUST_ID, c.FIRST_NAME, c.SURNAME, c.CUST_TYPE
                HAVING COUNT(*) > 5
                ORDER BY txn_count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            try:
                cursor.execute(query8)
                results = cursor.fetchall()
                logger.info("  High-transaction customers:")
                for cust_id, name, cust_type, txn_count in results:
                    logger.info(f"    {cust_id}: {name:40} | Type:{cust_type} | Accounts:{txn_count}")
            except Exception as e:
                logger.info(f"  ❌ Transaction check failed: {e}")
            
            logger.info("\n" + "=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    investigate_customer_fields()
