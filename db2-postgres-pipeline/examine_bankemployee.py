#!/usr/bin/env python3
"""
Examine BANKEMPLOYEE table structure and data
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def examine_bankemployee():
    """Examine BANKEMPLOYEE table structure and data"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("EXAMINING BANKEMPLOYEE TABLE")
            logger.info("=" * 80)
            
            # 1. Get table structure (simplified to avoid ODBC type issues)
            logger.info("\n🔍 BANKEMPLOYEE Table Structure:")
            query_structure = """
                SELECT 
                    COLNAME,
                    TYPENAME,
                    LENGTH,
                    SCALE,
                    NULLS,
                    COLNO
                FROM SYSCAT.COLUMNS
                WHERE TABSCHEMA = 'PROFITS'
                    AND TABNAME = 'BANKEMPLOYEE'
                ORDER BY COLNO
            """
            cursor.execute(query_structure)
            columns = cursor.fetchall()
            
            logger.info("  Column Name                | Type        | Length | Nulls")
            logger.info("  " + "-" * 60)
            for col_name, type_name, length, scale, nulls, colno in columns:
                logger.info(f"  {col_name:<25} | {type_name:<10} | {length:<6} | {nulls:<5}")
            
            # 2. Count total records
            logger.info(f"\n📊 Record Counts:")
            query_count = "SELECT COUNT(*) FROM BANKEMPLOYEE"
            cursor.execute(query_count)
            total_count = cursor.fetchone()[0]
            logger.info(f"  Total records: {total_count:,}")
            
            # Count by status
            query_status = """
                SELECT 
                    EMPL_STATUS,
                    COUNT(*) as count
                FROM BANKEMPLOYEE
                GROUP BY EMPL_STATUS
                ORDER BY count DESC
            """
            cursor.execute(query_status)
            status_counts = cursor.fetchall()
            
            logger.info("  By Status:")
            for status, count in status_counts:
                logger.info(f"    Status '{status}': {count:,} employees")
            
            # 3. Sample active employees
            logger.info(f"\n👥 Sample Active Employees (first 20):")
            query_sample = """
                SELECT 
                    ID,
                    FIRST_NAME,
                    LAST_NAME,
                    EMPL_STATUS,
                    DEPARTMENT,
                    POSITION,
                    BRANCH_CODE,
                    EMPLOYEE_NUMBER
                FROM BANKEMPLOYEE
                WHERE EMPL_STATUS = '1'
                ORDER BY ID
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query_sample)
            employees = cursor.fetchall()
            
            logger.info("  ID   | Name                    | Status | Department      | Position        | Branch | Emp#")
            logger.info("  " + "-" * 95)
            for emp_id, first_name, last_name, status, dept, position, branch, emp_num in employees:
                name = f"{first_name or ''} {last_name or ''}".strip()
                dept_display = dept[:15] if dept else ""
                pos_display = position[:15] if position else ""
                branch_display = branch if branch else ""
                emp_num_display = emp_num if emp_num else ""
                logger.info(f"  {emp_id:<4} | {name:<23} | {status:<6} | {dept_display:<15} | {pos_display:<15} | {branch_display:<6} | {emp_num_display}")
            
            # 4. Check departments
            logger.info(f"\n🏢 Departments:")
            query_dept = """
                SELECT 
                    DEPARTMENT,
                    COUNT(*) as count
                FROM BANKEMPLOYEE
                WHERE EMPL_STATUS = '1'
                    AND DEPARTMENT IS NOT NULL
                    AND TRIM(DEPARTMENT) != ''
                GROUP BY DEPARTMENT
                ORDER BY count DESC
                FETCH FIRST 15 ROWS ONLY
            """
            cursor.execute(query_dept)
            departments = cursor.fetchall()
            
            for dept, count in departments:
                logger.info(f"  {dept:<30}: {count:3} employees")
            
            # 5. Check positions
            logger.info(f"\n💼 Positions:")
            query_pos = """
                SELECT 
                    POSITION,
                    COUNT(*) as count
                FROM BANKEMPLOYEE
                WHERE EMPL_STATUS = '1'
                    AND POSITION IS NOT NULL
                    AND TRIM(POSITION) != ''
                GROUP BY POSITION
                ORDER BY count DESC
                FETCH FIRST 15 ROWS ONLY
            """
            cursor.execute(query_pos)
            positions = cursor.fetchall()
            
            for position, count in positions:
                logger.info(f"  {position:<30}: {count:3} employees")
            
            # 6. Check branch codes
            logger.info(f"\n🏦 Branch Codes:")
            query_branch = """
                SELECT 
                    BRANCH_CODE,
                    COUNT(*) as count
                FROM BANKEMPLOYEE
                WHERE EMPL_STATUS = '1'
                    AND BRANCH_CODE IS NOT NULL
                    AND TRIM(BRANCH_CODE) != ''
                GROUP BY BRANCH_CODE
                ORDER BY count DESC
                FETCH FIRST 15 ROWS ONLY
            """
            cursor.execute(query_branch)
            branches = cursor.fetchall()
            
            for branch, count in branches:
                logger.info(f"  Branch {branch:<20}: {count:3} employees")
            
            # 7. Look for agent-related employees
            logger.info(f"\n🔍 Agent-Related Employees:")
            query_agents = """
                SELECT 
                    ID,
                    FIRST_NAME,
                    LAST_NAME,
                    DEPARTMENT,
                    POSITION,
                    BRANCH_CODE
                FROM BANKEMPLOYEE
                WHERE EMPL_STATUS = '1'
                    AND (UPPER(DEPARTMENT) LIKE '%AGENT%'
                         OR UPPER(POSITION) LIKE '%AGENT%'
                         OR UPPER(FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(LAST_NAME) LIKE '%AGENT%'
                         OR UPPER(DEPARTMENT) LIKE '%MOBILE%'
                         OR UPPER(POSITION) LIKE '%MOBILE%'
                         OR UPPER(DEPARTMENT) LIKE '%CHANNEL%'
                         OR UPPER(POSITION) LIKE '%CHANNEL%')
                ORDER BY ID
            """
            cursor.execute(query_agents)
            agent_employees = cursor.fetchall()
            
            if agent_employees:
                logger.info("  Agent-related employees found:")
                logger.info("  ID   | Name                    | Department      | Position        | Branch")
                logger.info("  " + "-" * 75)
                for emp_id, first_name, last_name, dept, position, branch in agent_employees:
                    name = f"{first_name or ''} {last_name or ''}".strip()
                    dept_display = dept[:15] if dept else ""
                    pos_display = position[:15] if position else ""
                    branch_display = branch if branch else ""
                    logger.info(f"  {emp_id:<4} | {name:<23} | {dept_display:<15} | {pos_display:<15} | {branch_display}")
            else:
                logger.info("  No agent-related employees found")
            
            # 8. Check if any employees manage many customers (potential agent managers)
            logger.info(f"\n👨‍💼 Employees Managing Many Customers:")
            query_managers = """
                SELECT 
                    be.ID,
                    be.FIRST_NAME,
                    be.LAST_NAME,
                    be.DEPARTMENT,
                    be.POSITION,
                    COUNT(c.CUST_ID) as customer_count
                FROM BANKEMPLOYEE be
                JOIN CUSTOMER c ON c.FK_BANKEMPLOYEEID = be.ID
                WHERE be.EMPL_STATUS = '1'
                    AND c.ENTRY_STATUS = '1'
                GROUP BY be.ID, be.FIRST_NAME, be.LAST_NAME, be.DEPARTMENT, be.POSITION
                HAVING COUNT(c.CUST_ID) > 100
                ORDER BY customer_count DESC
                FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(query_managers)
            managers = cursor.fetchall()
            
            if managers:
                logger.info("  Employees managing >100 customers:")
                logger.info("  ID   | Name                    | Department      | Position        | Customers")
                logger.info("  " + "-" * 80)
                for emp_id, first_name, last_name, dept, position, cust_count in managers:
                    name = f"{first_name or ''} {last_name or ''}".strip()
                    dept_display = dept[:15] if dept else ""
                    pos_display = position[:15] if position else ""
                    logger.info(f"  {emp_id:<4} | {name:<23} | {dept_display:<15} | {pos_display:<15} | {cust_count:9}")
            else:
                logger.info("  No employees managing >100 customers found")
            
            # 9. Check if any specific employee manages the 65 found agents
            logger.info(f"\n🎯 Checking Employee Management of Found Agents:")
            
            # Get the customer IDs from our found agents
            found_agent_ids = [
                38988, 38971, 45117, 39572, 37538, 42488, 56431, 32799, 38208,
                61927, 8661, 60087, 26587, 51611, 40248, 59921, 43415, 34671,
                45012, 45186, 60723, 186, 50489, 62673, 47027, 42338, 47054,
                22410, 38480, 41480, 23958, 13692, 34967, 47283, 48297, 28651,
                26962, 25980, 8536, 57175, 39122, 48877, 51853, 60175, 52733,
                51893, 16765, 60611, 52592, 55606, 62310, 9368, 61335, 62098,
                60265, 61305, 32992, 52815
            ]
            
            agent_ids_str = ','.join(map(str, found_agent_ids))
            
            query_agent_managers = f"""
                SELECT 
                    be.ID,
                    be.FIRST_NAME,
                    be.LAST_NAME,
                    be.DEPARTMENT,
                    be.POSITION,
                    COUNT(c.CUST_ID) as agent_count
                FROM BANKEMPLOYEE be
                JOIN CUSTOMER c ON c.FK_BANKEMPLOYEEID = be.ID
                WHERE be.EMPL_STATUS = '1'
                    AND c.ENTRY_STATUS = '1'
                    AND c.CUST_ID IN ({agent_ids_str})
                GROUP BY be.ID, be.FIRST_NAME, be.LAST_NAME, be.DEPARTMENT, be.POSITION
                ORDER BY agent_count DESC
            """
            cursor.execute(query_agent_managers)
            agent_managers = cursor.fetchall()
            
            if agent_managers:
                logger.info("  Employees managing found agents:")
                logger.info("  ID   | Name                    | Department      | Position        | Agents")
                logger.info("  " + "-" * 75)
                for emp_id, first_name, last_name, dept, position, agent_count in agent_managers:
                    name = f"{first_name or ''} {last_name or ''}".strip()
                    dept_display = dept[:15] if dept else ""
                    pos_display = position[:15] if position else ""
                    logger.info(f"  {emp_id:<4} | {name:<23} | {dept_display:<15} | {pos_display:<15} | {agent_count:6}")
            else:
                logger.info("  No employees found managing the found agents")
            
            logger.info("\n" + "=" * 80)
            logger.info("💡 BANKEMPLOYEE ANALYSIS SUMMARY:")
            logger.info("=" * 80)
            logger.info(f"  Total employees: {total_count:,}")
            logger.info(f"  Active employees: {[count for status, count in status_counts if status == '1'][0] if any(status == '1' for status, count in status_counts) else 0:,}")
            logger.info(f"  Agent-related employees: {len(agent_employees)}")
            logger.info(f"  High-volume customer managers: {len(managers)}")
            logger.info(f"  Employees managing found agents: {len(agent_managers)}")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    examine_bankemployee()