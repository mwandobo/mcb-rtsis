#!/usr/bin/env python3
"""
Find actual mobile money agents or banking agents - not insurance companies or cooperatives
"""

from db2_connection import DB2Connection

def find_actual_agents():
    """Find real mobile money agents"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Look for customers with agent-like business names
        print("🔍 Looking for customers with agent-like business patterns:")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE (UPPER(FIRST_NAME) LIKE '%AGENT%'
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
              AND ENTRY_STATUS = '1'
              AND MOBILE_TEL IS NOT NULL
              AND MOBILE_TEL != ''
        """)
        agent_like = cursor.fetchone()[0]
        print(f"   Agent-like customers: {agent_like}")
        
        if agent_like > 0:
            cursor.execute("""
                SELECT CUST_ID, FIRST_NAME, SURNAME, MOBILE_TEL, CUST_TYPE, CUSTOMER_BEGIN_DAT
                FROM CUSTOMER 
                WHERE (UPPER(FIRST_NAME) LIKE '%AGENT%'
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
                  AND ENTRY_STATUS = '1'
                  AND MOBILE_TEL IS NOT NULL
                  AND MOBILE_TEL != ''
                FETCH FIRST 20 ROWS ONLY
            """)
            rows = cursor.fetchall()
            print("   Sample agent-like customers:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Mobile: {row[3]}, Type: {row[4]}")
        
        # Look for individual customers (CUST_TYPE = '1') with mobile numbers - these could be individual agents
        print(f"\n🔍 Individual customers with mobile numbers (potential individual agents):")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE CUST_TYPE = '1'
              AND ENTRY_STATUS = '1'
              AND MOBILE_TEL IS NOT NULL 
              AND MOBILE_TEL != ''
              AND LENGTH(TRIM(MOBILE_TEL)) > 5
        """)
        individual_customers = cursor.fetchone()[0]
        print(f"   Individual customers with mobile: {individual_customers:,}")
        
        if individual_customers > 0:
            cursor.execute("""
                SELECT CUST_ID, FIRST_NAME, SURNAME, MOBILE_TEL, CUSTOMER_BEGIN_DAT
                FROM CUSTOMER 
                WHERE CUST_TYPE = '1'
                  AND ENTRY_STATUS = '1'
                  AND MOBILE_TEL IS NOT NULL 
                  AND MOBILE_TEL != ''
                  AND LENGTH(TRIM(MOBILE_TEL)) > 5
                FETCH FIRST 15 ROWS ONLY
            """)
            rows = cursor.fetchall()
            print("   Sample individual customers:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Mobile: {row[3]}")
        
        # Check for any transaction tables that might indicate agent activity
        print(f"\n🔍 Looking for agent-related transaction patterns:")
        
        # Check if there are any tables with mobile money patterns
        try:
            cursor.execute("""
                SELECT TABNAME FROM SYSCAT.TABLES 
                WHERE TABSCHEMA = 'PROFITS' 
                  AND (TABNAME LIKE '%MOBILE%' 
                       OR TABNAME LIKE '%MPESA%'
                       OR TABNAME LIKE '%TIGO%'
                       OR TABNAME LIKE '%AIRTEL%'
                       OR TABNAME LIKE '%HALO%'
                       OR TABNAME LIKE '%WAKALA%'
                       OR TABNAME LIKE '%AGENT%'
                       OR TABNAME LIKE '%MNO%')
                ORDER BY TABNAME
            """)
            mobile_tables = cursor.fetchall()
            print("   Mobile money related tables:")
            for table in mobile_tables:
                print(f"   - {table[0]}")
                
                # Get count for each table
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    count = cursor.fetchone()[0]
                    print(f"     Records: {count:,}")
                    
                    # Show sample data for smaller tables
                    if count > 0 and count < 100:
                        cursor.execute(f"SELECT * FROM {table[0]} FETCH FIRST 3 ROWS ONLY")
                        sample_rows = cursor.fetchall()
                        print(f"     Sample data:")
                        for sample in sample_rows[:2]:
                            print(f"     {sample}")
                            
                except Exception as e:
                    print(f"     Error: {e}")
                    
        except Exception as e:
            print(f"   Error finding tables: {e}")
        
        # Look for customers with specific mobile number patterns (Tanzanian mobile operators)
        print(f"\n🔍 Customers with Tanzanian mobile number patterns:")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE ENTRY_STATUS = '1'
              AND (MOBILE_TEL LIKE '0754%'  -- Vodacom
                   OR MOBILE_TEL LIKE '0755%'  -- Vodacom
                   OR MOBILE_TEL LIKE '0756%'  -- Vodacom
                   OR MOBILE_TEL LIKE '0757%'  -- Vodacom
                   OR MOBILE_TEL LIKE '0714%'  -- Tigo
                   OR MOBILE_TEL LIKE '0715%'  -- Tigo
                   OR MOBILE_TEL LIKE '0716%'  -- Tigo
                   OR MOBILE_TEL LIKE '0717%'  -- Tigo
                   OR MOBILE_TEL LIKE '0684%'  -- Airtel
                   OR MOBILE_TEL LIKE '0685%'  -- Airtel
                   OR MOBILE_TEL LIKE '0686%'  -- Airtel
                   OR MOBILE_TEL LIKE '0687%'  -- Airtel
                   OR MOBILE_TEL LIKE '0621%'  -- Halotel
                   OR MOBILE_TEL LIKE '0622%'  -- Halotel
                   OR MOBILE_TEL LIKE '0623%') -- Halotel
        """)
        tz_mobile_customers = cursor.fetchone()[0]
        print(f"   Customers with TZ mobile patterns: {tz_mobile_customers:,}")
        
        # Check for any account types that might indicate agent accounts
        print(f"\n🔍 Looking for special account types or products:")
        try:
            cursor.execute("""
                SELECT TABNAME FROM SYSCAT.TABLES 
                WHERE TABSCHEMA = 'PROFITS' 
                  AND (TABNAME LIKE '%PRODUCT%' 
                       OR TABNAME LIKE '%ACCOUNT%'
                       OR TABNAME LIKE '%SERVICE%')
                ORDER BY TABNAME
            """)
            account_tables = cursor.fetchall()
            print("   Account/Product related tables:")
            for table in account_tables[:10]:  # Show first 10
                print(f"   - {table[0]}")
                
        except Exception as e:
            print(f"   Error: {e}")

if __name__ == "__main__":
    find_actual_agents()