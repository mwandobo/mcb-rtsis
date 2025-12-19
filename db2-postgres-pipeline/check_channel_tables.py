#!/usr/bin/env python3
"""
Check for channel tables that record bank employees and agent relationships
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_channel_tables():
    """Check for channel tables and bank employee relationships"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("CHECKING FOR CHANNEL TABLES AND BANK EMPLOYEE RECORDS")
            logger.info("=" * 80)
            
            # 1. First, let's see all tables that might be related to channels or employees
            logger.info("\n🔍 Searching for channel/employee related tables:")
            query1 = """
                SELECT TABNAME, TABSCHEMA, TYPE
                FROM SYSCAT.TABLES
                WHERE TABSCHEMA = 'PROFITS'
                    AND (UPPER(TABNAME) LIKE '%CHANNEL%'
                         OR UPPER(TABNAME) LIKE '%EMPLOYEE%'
                         OR UPPER(TABNAME) LIKE '%AGENT%'
                         OR UPPER(TABNAME) LIKE '%DISTR%'
                         OR UPPER(TABNAME) LIKE '%BANK%'
                         OR UPPER(TABNAME) LIKE '%STAFF%'
                         OR UPPER(TABNAME) LIKE '%USER%'
                         OR UPPER(TABNAME) LIKE '%USR%')
                ORDER BY TABNAME
            """
            cursor.execute(query1)
            tables = cursor.fetchall()
            
            if tables:
                logger.info("  Found related tables:")
                for table_name, schema, table_type in tables:
                    logger.info(f"    {schema}.{table_name} ({table_type})")
            else:
                logger.info("  No channel/employee related tables found")
            
            # 2. Check BANKEMPLOYEE table specifically
            logger.info("\n🔍 Checking BANKEMPLOYEE table:")
            try:
                query2 = """
                    SELECT COUNT(*) as total_count
                    FROM BANKEMPLOYEE
                """
                cursor.execute(query2)
                count = cursor.fetchone()[0]
                logger.info(f"  ✅ BANKEMPLOYEE table exists with {count:,} records")
                
                # Get sample bank employees
                query2b = """
                    SELECT 
                        ID,
                        FIRST_NAME,
                        LAST_NAME,
                        EMPL_STATUS,
                        EMPLOYEE_ID,
                        MOBILE_TEL,
                        FK_UNITID
                    FROM BANKEMPLOYEE
                    WHERE EMPL_STATUS = '1'
                    ORDER BY ID
                    FETCH FIRST 10 ROWS ONLY
                """
                cursor.execute(query2b)
                employees = cursor.fetchall()
                
                logger.info("  Sample active bank employees:")
                logger.info("    ID     | Name                    | EmpID    | Mobile          | Unit")
                logger.info("    " + "-" * 70)
                for emp_id, first_name, last_name, status, employee_id, mobile, unit_id in employees:
                    name = f"{first_name or ''} {last_name or ''}".strip()
                    mobile_display = mobile if mobile else "No Mobile"
                    emp_id_display = employee_id if employee_id else "No ID"
                    logger.info(f"    {emp_id:6} | {name:23} | {emp_id_display:8} | {mobile_display:15} | {unit_id}")
                
            except Exception as e:
                logger.info(f"  ❌ BANKEMPLOYEE table check failed: {e}")
            
            # 3. Check DISTR_CHANNEL table if it exists
            logger.info("\n🔍 Checking DISTR_CHANNEL table:")
            try:
                query3 = """
                    SELECT COUNT(*) as total_count
                    FROM DISTR_CHANNEL
                """
                cursor.execute(query3)
                count = cursor.fetchone()[0]
                logger.info(f"  ✅ DISTR_CHANNEL table exists with {count:,} records")
                
                # Get sample distribution channels
                query3b = """
                    SELECT 
                        ID,
                        DESCRIPTION,
                        ENTRY_STATUS
                    FROM DISTR_CHANNEL
                    ORDER BY ID
                    FETCH FIRST 10 ROWS ONLY
                """
                cursor.execute(query3b)
                channels = cursor.fetchall()
                
                logger.info("  Distribution channels:")
                for channel_id, description, status in channels:
                    status_text = "Active" if status == '1' else "Inactive"
                    logger.info(f"    {channel_id:3}: {description:40} ({status_text})")
                
            except Exception as e:
                logger.info(f"  ❌ DISTR_CHANNEL table check failed: {e}")
            
            # 4. Check if customers have distribution channel links
            logger.info("\n🔍 Checking customer-channel relationships:")
            try:
                query4 = """
                    SELECT 
                        c.FK_DISTR_CHANNEID,
                        COUNT(*) as customer_count
                    FROM CUSTOMER c
                    WHERE c.FK_DISTR_CHANNEID IS NOT NULL
                        AND c.ENTRY_STATUS = '1'
                    GROUP BY c.FK_DISTR_CHANNEID
                    ORDER BY customer_count DESC
                    FETCH FIRST 10 ROWS ONLY
                """
                cursor.execute(query4)
                channel_customers = cursor.fetchall()
                
                if channel_customers:
                    logger.info("  Customers by distribution channel:")
                    for channel_id, count in channel_customers:
                        logger.info(f"    Channel {channel_id}: {count:,} customers")
                else:
                    logger.info("  No customers linked to distribution channels")
                
            except Exception as e:
                logger.info(f"  ❌ Customer-channel relationship check failed: {e}")
            
            # 5. Check if agents have specific channel assignments
            logger.info("\n🔍 Checking agent-channel relationships:")
            try:
                query5 = """
                    SELECT 
                        c.CUST_ID,
                        TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                        c.FK_DISTR_CHANNEID,
                        dc.DESCRIPTION as channel_desc
                    FROM CUSTOMER c
                    LEFT JOIN DISTR_CHANNEL dc ON dc.ID = c.FK_DISTR_CHANNEID
                    WHERE c.ENTRY_STATUS = '1'
                        AND c.MOBILE_TEL IS NOT NULL
                        AND c.MOBILE_TEL != ''
                        AND (UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                             OR UPPER(c.SURNAME) LIKE '%AGENT%'
                             OR UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                             OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                             OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                             OR UPPER(c.SURNAME) LIKE '%DUKA%')
                    ORDER BY c.CUST_ID
                    FETCH FIRST 20 ROWS ONLY
                """
                cursor.execute(query5)
                agent_channels = cursor.fetchall()
                
                if agent_channels:
                    logger.info("  Agent-channel assignments:")
                    logger.info("    ID     | Name                    | Channel | Description")
                    logger.info("    " + "-" * 65)
                    for cust_id, name, channel_id, channel_desc in agent_channels:
                        channel_display = str(channel_id) if channel_id else "None"
                        desc_display = channel_desc if channel_desc else "No Description"
                        logger.info(f"    {cust_id:6} | {name:23} | {channel_display:7} | {desc_display}")
                else:
                    logger.info("  No agent-channel assignments found")
                
            except Exception as e:
                logger.info(f"  ❌ Agent-channel relationship check failed: {e}")
            
            # 6. Check if there are employee-customer relationships
            logger.info("\n🔍 Checking employee-customer relationships:")
            try:
                query6 = """
                    SELECT 
                        c.FK_BANKEMPLOYEEID,
                        COUNT(*) as customer_count
                    FROM CUSTOMER c
                    WHERE c.FK_BANKEMPLOYEEID IS NOT NULL
                        AND c.ENTRY_STATUS = '1'
                    GROUP BY c.FK_BANKEMPLOYEEID
                    ORDER BY customer_count DESC
                    FETCH FIRST 10 ROWS ONLY
                """
                cursor.execute(query6)
                emp_customers = cursor.fetchall()
                
                if emp_customers:
                    logger.info("  Customers by bank employee:")
                    for emp_id, count in emp_customers:
                        logger.info(f"    Employee {emp_id}: {count:,} customers")
                        
                    # Get details for top employee
                    top_emp_id = emp_customers[0][0]
                    query6b = """
                        SELECT 
                            be.FIRST_NAME,
                            be.LAST_NAME,
                            be.EMPLOYEE_ID,
                            COUNT(c.CUST_ID) as customer_count
                        FROM BANKEMPLOYEE be
                        LEFT JOIN CUSTOMER c ON c.FK_BANKEMPLOYEEID = be.ID
                        WHERE be.ID = ?
                            AND c.ENTRY_STATUS = '1'
                        GROUP BY be.FIRST_NAME, be.LAST_NAME, be.EMPLOYEE_ID
                    """
                    cursor.execute(query6b, (top_emp_id,))
                    emp_detail = cursor.fetchone()
                    if emp_detail:
                        first_name, last_name, employee_id, count = emp_detail
                        name = f"{first_name or ''} {last_name or ''}".strip()
                        logger.info(f"    Top employee: {name} (ID: {employee_id}) manages {count:,} customers")
                else:
                    logger.info("  No employee-customer relationships found")
                
            except Exception as e:
                logger.info(f"  ❌ Employee-customer relationship check failed: {e}")
            
            logger.info("\n" + "=" * 80)
            logger.info("💡 SUMMARY:")
            logger.info("  Channel and employee tables exist in the database")
            logger.info("  Bank employees are recorded in BANKEMPLOYEE table")
            logger.info("  Distribution channels are in DISTR_CHANNEL table")
            logger.info("  Customers can be linked to employees and channels")
            logger.info("  This could provide additional context for agent identification")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_channel_tables()