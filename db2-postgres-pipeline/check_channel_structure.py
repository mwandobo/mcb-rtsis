#!/usr/bin/env python3
"""
Check channel table structures and relationships
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_channel_structure():
    """Check channel table structures and relationships"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("CHECKING CHANNEL TABLE STRUCTURES AND RELATIONSHIPS")
            logger.info("=" * 80)
            
            # 1. Check DISTR_CHANNEL table structure
            logger.info("\n🔍 DISTR_CHANNEL table structure:")
            query1 = """
                SELECT 
                    COLNAME,
                    TYPENAME,
                    LENGTH
                FROM SYSCAT.COLUMNS
                WHERE TABSCHEMA = 'PROFITS'
                    AND TABNAME = 'DISTR_CHANNEL'
                ORDER BY COLNO
            """
            cursor.execute(query1)
            distr_columns = cursor.fetchall()
            
            logger.info("  Columns:")
            for col_name, type_name, length in distr_columns:
                logger.info(f"    {col_name:<30} {type_name:<15} {length}")
            
            # 2. Check DISTR_CHANNEL data
            logger.info("\n🔍 DISTR_CHANNEL data:")
            # Use the first column as ID
            id_column = distr_columns[0][0] if distr_columns else 'ID'
            
            query2 = f"""
                SELECT *
                FROM DISTR_CHANNEL
                ORDER BY {id_column}
                FETCH FIRST 15 ROWS ONLY
            """
            cursor.execute(query2)
            channels = cursor.fetchall()
            
            logger.info("  Distribution channels:")
            for channel in channels:
                logger.info(f"    {channel}")
            
            # 3. Check CUSTOMER_CHANNEL table
            logger.info("\n🔍 CUSTOMER_CHANNEL table:")
            try:
                query3 = """
                    SELECT COUNT(*) as count
                    FROM CUSTOMER_CHANNEL
                """
                cursor.execute(query3)
                count = cursor.fetchone()[0]
                logger.info(f"  ✅ CUSTOMER_CHANNEL table exists with {count:,} records")
                
                # Get structure
                query3b = """
                    SELECT 
                        COLNAME,
                        TYPENAME,
                        LENGTH
                    FROM SYSCAT.COLUMNS
                    WHERE TABSCHEMA = 'PROFITS'
                        AND TABNAME = 'CUSTOMER_CHANNEL'
                    ORDER BY COLNO
                """
                cursor.execute(query3b)
                cust_channel_columns = cursor.fetchall()
                
                logger.info("  Columns:")
                for col_name, type_name, length in cust_channel_columns:
                    logger.info(f"    {col_name:<30} {type_name:<15} {length}")
                
                # Sample data
                query3c = """
                    SELECT *
                    FROM CUSTOMER_CHANNEL
                    FETCH FIRST 10 ROWS ONLY
                """
                cursor.execute(query3c)
                cust_channels = cursor.fetchall()
                
                logger.info("  Sample customer-channel data:")
                for row in cust_channels:
                    logger.info(f"    {row}")
                
            except Exception as e:
                logger.info(f"  ❌ CUSTOMER_CHANNEL check failed: {e}")
            
            # 4. Check BANKEMPLOYEE table structure
            logger.info("\n🔍 BANKEMPLOYEE table structure:")
            query4 = """
                SELECT 
                    COLNAME,
                    TYPENAME,
                    LENGTH
                FROM SYSCAT.COLUMNS
                WHERE TABSCHEMA = 'PROFITS'
                    AND TABNAME = 'BANKEMPLOYEE'
                ORDER BY COLNO
            """
            cursor.execute(query4)
            emp_columns = cursor.fetchall()
            
            logger.info("  Columns:")
            for col_name, type_name, length in emp_columns:
                logger.info(f"    {col_name:<30} {type_name:<15} {length}")
            
            # 5. Check sample bank employees
            logger.info("\n🔍 Sample bank employees:")
            query5 = """
                SELECT *
                FROM BANKEMPLOYEE
                WHERE EMPL_STATUS = '1'
                FETCH FIRST 5 ROWS ONLY
            """
            cursor.execute(query5)
            employees = cursor.fetchall()
            
            for emp in employees:
                logger.info(f"    {emp}")
            
            # 6. Check if agents are linked to specific channels or employees
            logger.info("\n🔍 Checking agent relationships:")
            
            # Check if any of our known agents have channel assignments
            query6 = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.FK_DISTR_CHANNEID,
                    c.FK_BANKEMPLOYEEID
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND (UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                         OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                         OR UPPER(c.SURNAME) LIKE '%DUKA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(c.SURNAME) LIKE '%AGENT%')
                ORDER BY c.CUST_ID
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query6)
            agent_relations = cursor.fetchall()
            
            logger.info("  Agent channel/employee assignments:")
            logger.info("    ID     | Name                    | Channel | Employee")
            logger.info("    " + "-" * 60)
            for cust_id, name, channel_id, emp_id in agent_relations:
                channel_display = str(channel_id) if channel_id else "None"
                emp_display = str(emp_id) if emp_id else "None"
                logger.info(f"    {cust_id:6} | {name:23} | {channel_display:7} | {emp_display}")
            
            # 7. Check if there's a specific channel for agents
            logger.info("\n🔍 Checking for agent-specific channels:")
            
            # Count customers by channel for our agents
            query7 = """
                SELECT 
                    c.FK_DISTR_CHANNEID,
                    COUNT(*) as agent_count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.FK_DISTR_CHANNEID IS NOT NULL
                    AND (UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                         OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                         OR UPPER(c.SURNAME) LIKE '%DUKA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(c.SURNAME) LIKE '%AGENT%')
                GROUP BY c.FK_DISTR_CHANNEID
                ORDER BY agent_count DESC
            """
            cursor.execute(query7)
            agent_channels = cursor.fetchall()
            
            if agent_channels:
                logger.info("  Agents by channel:")
                for channel_id, count in agent_channels:
                    logger.info(f"    Channel {channel_id}: {count} agents")
            else:
                logger.info("  No agents assigned to specific channels")
            
            logger.info("\n" + "=" * 80)
            logger.info("💡 FINDINGS:")
            logger.info("  - DISTR_CHANNEL table contains distribution channel definitions")
            logger.info("  - CUSTOMER_CHANNEL table may link customers to channels")
            logger.info("  - BANKEMPLOYEE table contains bank staff information")
            logger.info("  - Customers can be linked to employees via FK_BANKEMPLOYEEID")
            logger.info("  - Customers can be linked to channels via FK_DISTR_CHANNEID")
            logger.info("  - This could help identify agents through channel assignments")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_channel_structure()