#!/usr/bin/env python3
"""
Find real mobile money agents or individual banking agents
"""

from db2_connection import DB2Connection

def find_real_agents():
    """Find actual agents, not bank terminals"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Look for customers who might be mobile money agents
        print("🔍 Looking for Mobile Money Agents in CUSTOMER table:")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE (UPPER(EMPLOYER) LIKE '%MOBILE%' 
                   OR UPPER(EMPLOYER) LIKE '%MONEY%'
                   OR UPPER(EMPLOYER) LIKE '%AGENT%'
                   OR UPPER(EMPLOYER) LIKE '%TIGO%'
                   OR UPPER(EMPLOYER) LIKE '%VODACOM%'
                   OR UPPER(EMPLOYER) LIKE '%AIRTEL%'
                   OR UPPER(EMPLOYER) LIKE '%HALOTEL%'
                   OR UPPER(EMPLOYER) LIKE '%MPESA%'
                   OR UPPER(EMPLOYER) LIKE '%TIGOPESA%'
                   OR UPPER(EMPLOYER) LIKE '%AIRTELMONEY%')
              AND ENTRY_STATUS = '1'
        """)
        mobile_agents = cursor.fetchone()[0]
        print(f"   Mobile money related customers: {mobile_agents}")
        
        if mobile_agents > 0:
            cursor.execute("""
                SELECT CUST_ID, FIRST_NAME, SURNAME, EMPLOYER, MOBILE_TEL, CUSTOMER_BEGIN_DAT
                FROM CUSTOMER 
                WHERE (UPPER(EMPLOYER) LIKE '%MOBILE%' 
                       OR UPPER(EMPLOYER) LIKE '%MONEY%'
                       OR UPPER(EMPLOYER) LIKE '%AGENT%'
                       OR UPPER(EMPLOYER) LIKE '%TIGO%'
                       OR UPPER(EMPLOYER) LIKE '%VODACOM%'
                       OR UPPER(EMPLOYER) LIKE '%AIRTEL%'
                       OR UPPER(EMPLOYER) LIKE '%HALOTEL%'
                       OR UPPER(EMPLOYER) LIKE '%MPESA%'
                       OR UPPER(EMPLOYER) LIKE '%TIGOPESA%'
                       OR UPPER(EMPLOYER) LIKE '%AIRTELMONEY%')
                  AND ENTRY_STATUS = '1'
                FETCH FIRST 15 ROWS ONLY
            """)
            rows = cursor.fetchall()
            print("   Sample mobile money agents:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Employer: {row[3][:40]}, Mobile: {row[4]}")
        
        # Look for customers with business type that might be agents
        print(f"\n🔍 Looking for Business Customers (Potential Agents):")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE CUST_TYPE = '2' 
              AND ENTRY_STATUS = '1'
              AND MOBILE_TEL IS NOT NULL 
              AND MOBILE_TEL != ''
        """)
        business_customers = cursor.fetchone()[0]
        print(f"   Business customers with mobile: {business_customers}")
        
        if business_customers > 0:
            cursor.execute("""
                SELECT CUST_ID, FIRST_NAME, SURNAME, EMPLOYER, MOBILE_TEL, CUSTOMER_BEGIN_DAT
                FROM CUSTOMER 
                WHERE CUST_TYPE = '2' 
                  AND ENTRY_STATUS = '1'
                  AND MOBILE_TEL IS NOT NULL 
                  AND MOBILE_TEL != ''
                FETCH FIRST 15 ROWS ONLY
            """)
            rows = cursor.fetchall()
            print("   Sample business customers:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Employer: {row[3][:40] if row[3] else 'N/A'}, Mobile: {row[4]}")
        
        # Check for any tables with TILL in the name that might contain agent data
        print(f"\n🔍 Investigating CASH_TILL for Agent Information:")
        cursor.execute("""
            SELECT TILL_NO, UNIT, FK_CURRENCYID_CURR, OPENING_BALANCE
            FROM CASH_TILL
            WHERE OPENING_BALANCE > 0
            FETCH FIRST 10 ROWS ONLY
        """)
        active_tills = cursor.fetchall()
        print("   Active tills with balance:")
        for i, till in enumerate(active_tills, 1):
            print(f"   {i}. TILL: {till[0]}, UNIT: {till[1]}, CURRENCY: {till[2]}, BALANCE: {till[3]}")
        
        # Look for any transaction tables that might have agent information
        print(f"\n🔍 Looking for Agent-related Transaction Tables:")
        
        # Check if there are any tables with patterns like AGENT_TRX, MOBILE_TRX, etc.
        try:
            cursor.execute("""
                SELECT TABNAME FROM SYSCAT.TABLES 
                WHERE TABSCHEMA = 'PROFITS' 
                  AND (TABNAME LIKE '%AGENT%' 
                       OR TABNAME LIKE '%MOBILE%' 
                       OR TABNAME LIKE '%TILL%'
                       OR TABNAME LIKE '%MNO%')
                ORDER BY TABNAME
            """)
            agent_tables = cursor.fetchall()
            print("   Agent-related tables found:")
            for table in agent_tables:
                print(f"   - {table[0]}")
                
                # Get count for each table
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    count = cursor.fetchone()[0]
                    print(f"     Records: {count:,}")
                except Exception as e:
                    print(f"     Error counting: {e}")
                    
        except Exception as e:
            print(f"   Error finding tables: {e}")
        
        # Check for customers with specific patterns that might indicate agents
        print(f"\n🔍 Customers with Agent-like Patterns:")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE (UPPER(FIRST_NAME) LIKE '%SHOP%'
                   OR UPPER(FIRST_NAME) LIKE '%STORE%'
                   OR UPPER(FIRST_NAME) LIKE '%DUKA%'
                   OR UPPER(SURNAME) LIKE '%SHOP%'
                   OR UPPER(SURNAME) LIKE '%STORE%'
                   OR UPPER(SURNAME) LIKE '%DUKA%'
                   OR UPPER(SURNAME) LIKE '%ENTERPRISE%'
                   OR UPPER(SURNAME) LIKE '%BUSINESS%')
              AND ENTRY_STATUS = '1'
              AND MOBILE_TEL IS NOT NULL
        """)
        shop_customers = cursor.fetchone()[0]
        print(f"   Shop/business-like customers: {shop_customers}")
        
        if shop_customers > 0:
            cursor.execute("""
                SELECT CUST_ID, FIRST_NAME, SURNAME, MOBILE_TEL, CUSTOMER_BEGIN_DAT
                FROM CUSTOMER 
                WHERE (UPPER(FIRST_NAME) LIKE '%SHOP%'
                       OR UPPER(FIRST_NAME) LIKE '%STORE%'
                       OR UPPER(FIRST_NAME) LIKE '%DUKA%'
                       OR UPPER(SURNAME) LIKE '%SHOP%'
                       OR UPPER(SURNAME) LIKE '%STORE%'
                       OR UPPER(SURNAME) LIKE '%DUKA%'
                       OR UPPER(SURNAME) LIKE '%ENTERPRISE%'
                       OR UPPER(SURNAME) LIKE '%BUSINESS%')
                  AND ENTRY_STATUS = '1'
                  AND MOBILE_TEL IS NOT NULL
                FETCH FIRST 10 ROWS ONLY
            """)
            rows = cursor.fetchall()
            print("   Sample shop/business customers:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Mobile: {row[3]}")

if __name__ == "__main__":
    find_real_agents()