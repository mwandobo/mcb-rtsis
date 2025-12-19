#!/usr/bin/env python3
"""
Find a more reliable method to identify agents instead of name patterns
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_reliable_agent_method():
    """Find a more reliable method to identify agents"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("FINDING RELIABLE AGENT IDENTIFICATION METHOD")
            logger.info("=" * 80)
            
            # 1. Check if there are customers assigned to AGENCY channel (9950)
            logger.info("\n🔍 Method 1: Check customers assigned to AGENCY channel (9950):")
            query1 = """
                SELECT COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.FK_DISTR_CHANNEID = 9950
                    AND c.ENTRY_STATUS = '1'
            """
            cursor.execute(query1)
            agency_count = cursor.fetchone()[0]
            logger.info(f"  Customers assigned to AGENCY channel: {agency_count:,}")
            
            if agency_count > 0:
                query1b = """
                    SELECT 
                        c.CUST_ID,
                        TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                        c.CUST_TYPE,
                        c.MOBILE_TEL,
                        c.CUSTOMER_BEGIN_DAT
                    FROM CUSTOMER c
                    WHERE c.FK_DISTR_CHANNEID = 9950
                        AND c.ENTRY_STATUS = '1'
                    ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                    FETCH FIRST 20 ROWS ONLY
                """
                cursor.execute(query1b)
                agency_customers = cursor.fetchall()
                
                logger.info("  Sample AGENCY channel customers:")
                for cust_id, name, cust_type, mobile, begin_date in agency_customers:
                    mobile_display = mobile if mobile else "No Mobile"
                    logger.info(f"    {cust_id:6}: {name:30} | Type: {cust_type} | {mobile_display:15} | {begin_date}")
            
            # 2. Check AGENT table for actual agent records
            logger.info("\n🔍 Method 2: Check AGENT table for actual agent records:")
            try:
                query2 = """
                    SELECT COUNT(*) as count
                    FROM AGENT
                    WHERE ENTRY_STATUS = '1'
                """
                cursor.execute(query2)
                agent_count = cursor.fetchone()[0]
                logger.info(f"  Active records in AGENT table: {agent_count:,}")
                
                if agent_count > 0:
                    # Get AGENT table structure
                    query2a = """
                        SELECT 
                            COLNAME,
                            TYPENAME,
                            LENGTH
                        FROM SYSCAT.COLUMNS
                        WHERE TABSCHEMA = 'PROFITS'
                            AND TABNAME = 'AGENT'
                        ORDER BY COLNO
                    """
                    cursor.execute(query2a)
                    agent_columns = cursor.fetchall()
                    
                    logger.info("  AGENT table columns:")
                    for col_name, type_name, length in agent_columns:
                        logger.info(f"    {col_name:<30} {type_name:<15} {length}")
                    
                    # Get sample agent records
                    query2b = """
                        SELECT *
                        FROM AGENT
                        WHERE ENTRY_STATUS = '1'
                        FETCH FIRST 10 ROWS ONLY
                    """
                    cursor.execute(query2b)
                    agents = cursor.fetchall()
                    
                    logger.info("  Sample AGENT records:")
                    for agent in agents:
                        logger.info(f"    {agent}")
                
            except Exception as e:
                logger.info(f"  ❌ AGENT table check failed: {e}")
            
            # 3. Check AGENT_TERMINAL table
            logger.info("\n🔍 Method 3: Check AGENT_TERMINAL table:")
            try:
                query3 = """
                    SELECT COUNT(*) as count
                    FROM AGENT_TERMINAL
                    WHERE ENTRY_STATUS = '1'
                """
                cursor.execute(query3)
                terminal_count = cursor.fetchone()[0]
                logger.info(f"  Active records in AGENT_TERMINAL table: {terminal_count:,}")
                
                if terminal_count > 0:
                    # Get structure
                    query3a = """
                        SELECT 
                            COLNAME,
                            TYPENAME,
                            LENGTH
                        FROM SYSCAT.COLUMNS
                        WHERE TABSCHEMA = 'PROFITS'
                            AND TABNAME = 'AGENT_TERMINAL'
                        ORDER BY COLNO
                    """
                    cursor.execute(query3a)
                    terminal_columns = cursor.fetchall()
                    
                    logger.info("  AGENT_TERMINAL table columns:")
                    for col_name, type_name, length in terminal_columns:
                        logger.info(f"    {col_name:<30} {type_name:<15} {length}")
                    
                    # Sample records
                    query3b = """
                        SELECT *
                        FROM AGENT_TERMINAL
                        WHERE ENTRY_STATUS = '1'
                        FETCH FIRST 5 ROWS ONLY
                    """
                    cursor.execute(query3b)
                    terminals = cursor.fetchall()
                    
                    logger.info("  Sample AGENT_TERMINAL records:")
                    for terminal in terminals:
                        logger.info(f"    {terminal}")
                
            except Exception as e:
                logger.info(f"  ❌ AGENT_TERMINAL table check failed: {e}")
            
            # 4. Check if there are specific employee codes that manage agents
            logger.info("\n🔍 Method 4: Check employee patterns for agent managers:")
            query4 = """
                SELECT 
                    c.FK_BANKEMPLOYEEID,
                    be.FIRST_NAME,
                    be.LAST_NAME,
                    COUNT(*) as customer_count,
                    COUNT(CASE WHEN c.MOBILE_TEL IS NOT NULL AND c.MOBILE_TEL != '' THEN 1 END) as mobile_count
                FROM CUSTOMER c
                JOIN BANKEMPLOYEE be ON be.ID = c.FK_BANKEMPLOYEEID
                WHERE c.ENTRY_STATUS = '1'
                    AND be.EMPL_STATUS = '1'
                GROUP BY c.FK_BANKEMPLOYEEID, be.FIRST_NAME, be.LAST_NAME
                HAVING COUNT(*) > 1000
                ORDER BY customer_count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query4)
            emp_stats = cursor.fetchall()
            
            logger.info("  Top employee customer managers:")
            logger.info("    Employee | Name                    | Customers | With Mobile")
            logger.info("    " + "-" * 65)
            for emp_id, first_name, last_name, cust_count, mobile_count in emp_stats:
                name = f"{first_name or ''} {last_name or ''}".strip()
                logger.info(f"    {emp_id:8} | {name:23} | {cust_count:9} | {mobile_count:11}")
            
            # 5. Check for transaction patterns that might indicate agents
            logger.info("\n🔍 Method 5: Check transaction patterns for high-volume customers:")
            try:
                query5 = """
                    SELECT 
                        c.CUST_ID,
                        TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                        c.CUST_TYPE,
                        c.MOBILE_TEL,
                        COUNT(gte.TRX_ID) as transaction_count
                    FROM CUSTOMER c
                    JOIN GLI_TRX_EXTRACT gte ON gte.CUST_ID = c.CUST_ID
                    WHERE c.ENTRY_STATUS = '1'
                        AND c.MOBILE_TEL IS NOT NULL
                        AND c.MOBILE_TEL != ''
                        AND gte.TRN_DATE >= DATE('2024-01-01')
                    GROUP BY c.CUST_ID, c.FIRST_NAME, c.SURNAME, c.CUST_TYPE, c.MOBILE_TEL
                    HAVING COUNT(gte.TRX_ID) > 1000
                    ORDER BY transaction_count DESC
                    FETCH FIRST 20 ROWS ONLY
                """
                cursor.execute(query5)
                high_volume = cursor.fetchall()
                
                if high_volume:
                    logger.info("  High-volume transaction customers (potential agents):")
                    logger.info("    ID     | Name                    | Type | Mobile          | Transactions")
                    logger.info("    " + "-" * 80)
                    for cust_id, name, cust_type, mobile, txn_count in high_volume:
                        logger.info(f"    {cust_id:6} | {name:23} | {cust_type:4} | {mobile:15} | {txn_count:12}")
                else:
                    logger.info("  No high-volume transaction customers found")
                
            except Exception as e:
                logger.info(f"  ❌ Transaction pattern check failed: {e}")
            
            # 6. Check for specific account types that might indicate agents
            logger.info("\n🔍 Method 6: Check for agent-specific account types:")
            try:
                query6 = """
                    SELECT 
                        pt.DESCRIPTION as product_type,
                        COUNT(DISTINCT c.CUST_ID) as customer_count
                    FROM CUSTOMER c
                    JOIN ACCOUNT a ON a.FK_CUSTOMERID = c.CUST_ID
                    JOIN PRODUCT_TYPE pt ON pt.ID = a.FK_PRODUCT_TYPEID
                    WHERE c.ENTRY_STATUS = '1'
                        AND a.ENTRY_STATUS = '1'
                        AND c.MOBILE_TEL IS NOT NULL
                        AND c.MOBILE_TEL != ''
                        AND (UPPER(pt.DESCRIPTION) LIKE '%AGENT%'
                             OR UPPER(pt.DESCRIPTION) LIKE '%MOBILE%'
                             OR UPPER(pt.DESCRIPTION) LIKE '%BUSINESS%')
                    GROUP BY pt.DESCRIPTION
                    ORDER BY customer_count DESC
                """
                cursor.execute(query6)
                product_types = cursor.fetchall()
                
                if product_types:
                    logger.info("  Agent-related product types:")
                    for product_desc, count in product_types:
                        logger.info(f"    {product_desc:40}: {count:4} customers")
                else:
                    logger.info("  No agent-related product types found")
                
            except Exception as e:
                logger.info(f"  ❌ Product type check failed: {e}")
            
            # Summary and recommendations
            logger.info("\n" + "=" * 80)
            logger.info("💡 RELIABLE AGENT IDENTIFICATION RECOMMENDATIONS:")
            logger.info("=" * 80)
            
            if agency_count > 0:
                logger.info("✅ BEST METHOD: Use FK_DISTR_CHANNEID = 9950 (AGENCY channel)")
                logger.info("   This is the most reliable method as it's specifically for agents")
            elif high_volume:
                logger.info("✅ ALTERNATIVE: Use high transaction volume customers")
                logger.info("   Customers with >1000 transactions in 2024 are likely agents")
            else:
                logger.info("❌ NO RELIABLE METHOD FOUND")
                logger.info("   May need to use a combination of approaches:")
                logger.info("   1. Specific employee assignments")
                logger.info("   2. Account product types")
                logger.info("   3. Transaction patterns")
                logger.info("   4. Manual agent list from bank")
            
            logger.info("\n🚫 AVOID: Name-based patterns (WAKALA, DUKA, etc.)")
            logger.info("   These are unreliable and may include non-agents")
            
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_reliable_agent_method()