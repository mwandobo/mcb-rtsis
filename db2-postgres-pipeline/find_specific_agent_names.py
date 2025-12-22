#!/usr/bin/env python3
"""
Find customers with specific agent names from the image
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_specific_agent_names():
    """Find customers with specific agent names from the provided list"""
    
    db2_conn = DB2Connection()
    
    # Names from the image
    agent_names = [
        "ISHISHA MIN SHOP AGENT",
        "LARRIES BUSINESS SERVICE", 
        "MBEVI STATIONERY AND GENERAL SERVICES",
        "NKOMEY MICROFINANCE"
    ]
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("SEARCHING FOR SPECIFIC AGENT NAMES FROM IMAGE")
            logger.info("=" * 80)
            
            total_found = 0
            
            for agent_name in agent_names:
                logger.info(f"\n🔍 Searching for: {agent_name}")
                logger.info("-" * 60)
                
                # Split the name into parts for flexible searching
                name_parts = agent_name.split()
                
                # Build search conditions for each part
                conditions = []
                for part in name_parts:
                    conditions.append(f"UPPER(c.FIRST_NAME) LIKE '%{part.upper()}%'")
                    conditions.append(f"UPPER(c.MIDDLE_NAME) LIKE '%{part.upper()}%'")
                    conditions.append(f"UPPER(c.SURNAME) LIKE '%{part.upper()}%'")
                
                # Create query with OR conditions
                where_clause = " OR ".join(conditions)
                
                query = f"""
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
                        c.CUSTOMER_BEGIN_DAT
                    FROM CUSTOMER c
                    WHERE ({where_clause})
                    ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                    FETCH FIRST 10 ROWS ONLY
                """
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    logger.info(f"  ✅ Found {len(results)} match(es):")
                    for row in results:
                        cust_id, first_name, middle_name, surname, full_name, cust_type, entry_status, mobile_tel, business_ind, begin_date = row
                        
                        status_text = "Active" if entry_status == '1' else "Inactive"
                        mobile_display = mobile_tel if mobile_tel else "No Mobile"
                        
                        logger.info(f"    ID: {cust_id:6} | {full_name:40} | Type: {cust_type} | {status_text:8} | {mobile_display:15} | Bus: {business_ind} | {begin_date}")
                        total_found += 1
                else:
                    logger.info("  ❌ No matches found")
            
            # Now search for more comprehensive agent patterns
            logger.info(f"\n" + "=" * 80)
            logger.info("COMPREHENSIVE AGENT PATTERN SEARCH")
            logger.info("=" * 80)
            
            comprehensive_query = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                    c.CUST_TYPE,
                    c.ENTRY_STATUS,
                    c.MOBILE_TEL,
                    c.BUSINESS_IND,
                    c.CUSTOMER_BEGIN_DAT
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND LENGTH(TRIM(c.MOBILE_TEL)) > 5
                    AND (UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(c.SURNAME) LIKE '%AGENT%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%AGENT%'
                         OR UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                         OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%WAKALA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                         OR UPPER(c.SURNAME) LIKE '%DUKA%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%DUKA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%SHOP%'
                         OR UPPER(c.SURNAME) LIKE '%SHOP%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%SHOP%'
                         OR UPPER(c.FIRST_NAME) LIKE '%STORE%'
                         OR UPPER(c.SURNAME) LIKE '%STORE%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%STORE%'
                         OR UPPER(c.FIRST_NAME) LIKE '%SERVICE%'
                         OR UPPER(c.SURNAME) LIKE '%SERVICE%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%SERVICE%'
                         OR UPPER(c.FIRST_NAME) LIKE '%BUSINESS%'
                         OR UPPER(c.SURNAME) LIKE '%BUSINESS%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%BUSINESS%'
                         OR UPPER(c.FIRST_NAME) LIKE '%MICROFINANCE%'
                         OR UPPER(c.SURNAME) LIKE '%MICROFINANCE%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%MICROFINANCE%'
                         OR UPPER(c.FIRST_NAME) LIKE '%STATIONERY%'
                         OR UPPER(c.SURNAME) LIKE '%STATIONERY%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%STATIONERY%')
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 50 ROWS ONLY
            """
            
            cursor.execute(comprehensive_query)
            comprehensive_results = cursor.fetchall()
            
            logger.info(f"\n📊 Found {len(comprehensive_results)} potential agents with comprehensive search:")
            logger.info("-" * 80)
            logger.info("ID     | Name                                     | Type | Status   | Mobile          | Bus | Date")
            logger.info("-" * 80)
            
            cust_type_1_count = 0
            cust_type_2_count = 0
            
            for row in comprehensive_results:
                cust_id, full_name, cust_type, entry_status, mobile_tel, business_ind, begin_date = row
                
                status_text = "Active" if entry_status == '1' else "Inactive"
                mobile_display = mobile_tel if mobile_tel else "No Mobile"
                
                if cust_type == '1':
                    cust_type_1_count += 1
                elif cust_type == '2':
                    cust_type_2_count += 1
                
                logger.info(f"{cust_id:6} | {full_name:40} | {cust_type:4} | {status_text:8} | {mobile_display:15} | {business_ind:3} | {begin_date}")
            
            # Summary
            logger.info("\n" + "=" * 80)
            logger.info("📊 SUMMARY:")
            logger.info(f"  Specific names found: {total_found}")
            logger.info(f"  Comprehensive search found: {len(comprehensive_results)}")
            logger.info(f"  CUST_TYPE '1' (Individual): {cust_type_1_count}")
            logger.info(f"  CUST_TYPE '2' (Corporate): {cust_type_2_count}")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_specific_agent_names()