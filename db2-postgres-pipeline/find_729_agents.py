#!/usr/bin/env python3
"""
Find the 729 agents mentioned by the user
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_729_agents():
    """Find where the 729 agents are stored"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("SEARCHING FOR THE 729 AGENTS")
            logger.info("=" * 80)
            
            # 1. Check AGENT_TERMINAL table - maybe these are the 729 agents
            logger.info("\n🔍 Method 1: Check AGENT_TERMINAL table (667 records):")
            query1 = """
                SELECT 
                    at.FK_AGENT_CUST_ID,
                    COUNT(*) as terminal_count
                FROM AGENT_TERMINAL at
                WHERE at.ENTRY_STATUS = '1'
                GROUP BY at.FK_AGENT_CUST_ID
                ORDER BY terminal_count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query1)
            agent_terminals = cursor.fetchall()
            
            unique_agents_from_terminals = len(agent_terminals)
            logger.info(f"  Unique agent customer IDs in AGENT_TERMINAL: {unique_agents_from_terminals}")
            
            logger.info("  Top agents by terminal count:")
            for agent_cust_id, terminal_count in agent_terminals:
                logger.info(f"    Agent Customer ID {agent_cust_id}: {terminal_count} terminals")
            
            # Get customer details for these agent IDs
            if agent_terminals:
                agent_ids = [str(row[0]) for row in agent_terminals[:10]]
                agent_ids_str = ','.join(agent_ids)
                
                query1b = f"""
                    SELECT 
                        c.CUST_ID,
                        TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                        c.CUST_TYPE,
                        c.MOBILE_TEL,
                        COUNT(at.USER_CODE) as terminal_count
                    FROM CUSTOMER c
                    LEFT JOIN AGENT_TERMINAL at ON at.FK_AGENT_CUST_ID = c.CUST_ID
                    WHERE c.CUST_ID IN ({agent_ids_str})
                        AND c.ENTRY_STATUS = '1'
                    GROUP BY c.CUST_ID, c.FIRST_NAME, c.SURNAME, c.CUST_TYPE, c.MOBILE_TEL
                    ORDER BY terminal_count DESC
                """
                cursor.execute(query1b)
                agent_customers = cursor.fetchall()
                
                logger.info("  Agent customer details:")
                logger.info("    ID     | Name                    | Type | Mobile          | Terminals")
                logger.info("    " + "-" * 75)
                for cust_id, name, cust_type, mobile, term_count in agent_customers:
                    mobile_display = mobile if mobile else "No Mobile"
                    logger.info(f"    {cust_id:6} | {name:23} | {cust_type:4} | {mobile_display:15} | {term_count:9}")
            
            # 2. Check if there are other agent-related tables
            logger.info("\n🔍 Method 2: Search for other agent-related tables:")
            query2 = """
                SELECT TABNAME, TABSCHEMA
                FROM SYSCAT.TABLES
                WHERE TABSCHEMA = 'PROFITS'
                    AND (UPPER(TABNAME) LIKE '%AGENT%'
                         OR UPPER(TABNAME) LIKE '%MOBILE%'
                         OR UPPER(TABNAME) LIKE '%POS%'
                         OR UPPER(TABNAME) LIKE '%TERMINAL%')
                ORDER BY TABNAME
            """
            cursor.execute(query2)
            agent_tables = cursor.fetchall()
            
            logger.info("  Agent-related tables found:")
            for table_name, schema in agent_tables:
                try:
                    count_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
                    cursor.execute(count_query)
                    count = cursor.fetchone()[0]
                    logger.info(f"    {table_name:30}: {count:6} records")
                except Exception as e:
                    logger.info(f"    {table_name:30}: Error - {e}")
            
            # 3. Check if there's a specific customer segment or category for agents
            logger.info("\n🔍 Method 3: Check customer segments/categories:")
            
            # Check CUST_STATUS patterns
            query3a = """
                SELECT 
                    c.CUST_STATUS,
                    COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                GROUP BY c.CUST_STATUS
                ORDER BY count DESC
            """
            cursor.execute(query3a)
            status_counts = cursor.fetchall()
            
            logger.info("  Customer status distribution (with mobile):")
            for status, count in status_counts:
                logger.info(f"    CUST_STATUS '{status}': {count:,} customers")
                if count == 729:
                    logger.info(f"    *** POTENTIAL MATCH: {count} customers ***")
            
            # Check SEGM_FLAGS patterns
            query3b = """
                SELECT 
                    c.SEGM_FLAGS,
                    COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND c.SEGM_FLAGS IS NOT NULL
                    AND TRIM(c.SEGM_FLAGS) != ''
                GROUP BY c.SEGM_FLAGS
                ORDER BY count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query3b)
            segm_counts = cursor.fetchall()
            
            logger.info("  Customer segment flags (with mobile):")
            for segm_flag, count in segm_counts:
                logger.info(f"    SEGM_FLAGS '{segm_flag}': {count:,} customers")
                if count == 729:
                    logger.info(f"    *** POTENTIAL MATCH: {count} customers ***")
            
            # 4. Check for customers with specific unit assignments
            logger.info("\n🔍 Method 4: Check unit assignments:")
            query4 = """
                SELECT 
                    c.FKUNIT_BELONGS,
                    COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                GROUP BY c.FKUNIT_BELONGS
                ORDER BY count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query4)
            unit_counts = cursor.fetchall()
            
            logger.info("  Customer unit assignments (with mobile):")
            for unit, count in unit_counts:
                logger.info(f"    Unit {unit}: {count:,} customers")
                if count == 729:
                    logger.info(f"    *** POTENTIAL MATCH: {count} customers ***")
            
            # 5. Check for customers managed by specific employees
            logger.info("\n🔍 Method 5: Check employee assignments:")
            query5 = """
                SELECT 
                    c.FK_BANKEMPLOYEEID,
                    COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                GROUP BY c.FK_BANKEMPLOYEEID
                ORDER BY count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query5)
            emp_counts = cursor.fetchall()
            
            logger.info("  Customer employee assignments (with mobile):")
            for emp_id, count in emp_counts:
                logger.info(f"    Employee {emp_id}: {count:,} customers")
                if count == 729:
                    logger.info(f"    *** POTENTIAL MATCH: {count} customers ***")
            
            # 6. Check for customers with specific business indicators
            logger.info("\n🔍 Method 6: Check business indicators:")
            query6 = """
                SELECT 
                    c.BUSINESS_IND,
                    c.VIP_IND,
                    COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                GROUP BY c.BUSINESS_IND, c.VIP_IND
                ORDER BY count DESC
                FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(query6)
            business_counts = cursor.fetchall()
            
            logger.info("  Customer business/VIP indicators (with mobile):")
            for business_ind, vip_ind, count in business_counts:
                logger.info(f"    BUSINESS_IND='{business_ind}', VIP_IND='{vip_ind}': {count:,} customers")
                if count == 729:
                    logger.info(f"    *** POTENTIAL MATCH: {count} customers ***")
            
            # 7. Check for customers with specific date ranges
            logger.info("\n🔍 Method 7: Check registration date patterns:")
            query7 = """
                SELECT 
                    YEAR(c.CUSTOMER_BEGIN_DAT) as reg_year,
                    COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND c.CUSTOMER_BEGIN_DAT IS NOT NULL
                GROUP BY YEAR(c.CUSTOMER_BEGIN_DAT)
                ORDER BY count DESC
                FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(query7)
            year_counts = cursor.fetchall()
            
            logger.info("  Customer registration by year (with mobile):")
            for year, count in year_counts:
                logger.info(f"    Year {year}: {count:,} customers")
                if count == 729:
                    logger.info(f"    *** POTENTIAL MATCH: {count} customers ***")
            
            # Summary
            logger.info("\n" + "=" * 80)
            logger.info("💡 SEARCH RESULTS FOR 729 AGENTS:")
            logger.info("=" * 80)
            logger.info("  - AGENT table: 2 records (not the 729)")
            logger.info("  - AGENT_TERMINAL table: 667 records (close but not 729)")
            logger.info(f"  - Unique agents from terminals: {unique_agents_from_terminals}")
            logger.info("  - Need to check if any field combination gives exactly 729")
            logger.info("  - Look for *** POTENTIAL MATCH *** markers above")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_729_agents()