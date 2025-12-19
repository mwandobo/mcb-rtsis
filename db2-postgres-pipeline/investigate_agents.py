#!/usr/bin/env python3
"""
Investigate agent-related tables in DB2 database
"""

from db2_connection import DB2Connection

def investigate_agents():
    """Investigate agent tables"""
    db2_conn = DB2Connection()
    
    # Connect to DB2
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Check AGENT_TERMINAL count and sample data
        print("🔍 AGENT_TERMINAL Investigation:")
        cursor.execute("SELECT COUNT(*) FROM AGENT_TERMINAL")
        count = cursor.fetchone()[0]
        print(f"   Records: {count:,}")
        
        if count > 0:
            cursor.execute("SELECT * FROM AGENT_TERMINAL FETCH FIRST 5 ROWS ONLY")
            rows = cursor.fetchall()
            print("   Sample data:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. USER_CODE: {row[0]}, LOCATION: {row[3]}, FK_AGENT_CUST_ID: {row[15]}")
        
        # Check CUSTOMER table for potential agents
        print("\n🔍 CUSTOMER Investigation (looking for agent-like customers):")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE UPPER(FIRST_NAME) LIKE '%AGENT%' 
               OR UPPER(SURNAME) LIKE '%AGENT%'
               OR UPPER(EMPLOYER) LIKE '%AGENT%'
               OR CUST_TYPE = 'A'
        """)
        agent_customers = cursor.fetchone()[0]
        print(f"   Agent-like customers: {agent_customers:,}")
        
        if agent_customers > 0:
            cursor.execute("""
                SELECT CUST_ID, FIRST_NAME, SURNAME, CUST_TYPE, EMPLOYER 
                FROM CUSTOMER 
                WHERE UPPER(FIRST_NAME) LIKE '%AGENT%' 
                   OR UPPER(SURNAME) LIKE '%AGENT%'
                   OR UPPER(EMPLOYER) LIKE '%AGENT%'
                   OR CUST_TYPE = 'A'
                FETCH FIRST 10 ROWS ONLY
            """)
            rows = cursor.fetchall()
            print("   Sample agent-like customers:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Type: {row[3]}, Employer: {row[4]}")
        
        # Check for mobile money related tables
        print("\n🔍 Mobile Money Investigation:")
        
        # Check if there are tables with MOBILE, TILL, or similar patterns
        tables_to_check = [
            "CASH_TILL",
            "CENTELOANS_MOBILE", 
            "DISTR_CHANNEL"
        ]
        
        for table in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   {table}: {count:,} records")
                
                if count > 0 and count < 1000:  # Show sample for smaller tables
                    cursor.execute(f"SELECT * FROM {table} FETCH FIRST 3 ROWS ONLY")
                    rows = cursor.fetchall()
                    print(f"     Sample data from {table}:")
                    for row in rows[:2]:  # Show first 2 rows
                        print(f"     {row}")
            except Exception as e:
                print(f"   {table}: Error - {e}")
        
        # Check DISTR_CHANNEL for agent channels
        print("\n🔍 DISTR_CHANNEL Investigation:")
        try:
            cursor.execute("SELECT * FROM DISTR_CHANNEL FETCH FIRST 10 ROWS ONLY")
            rows = cursor.fetchall()
            print("   Distribution channels:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Description: {row[1] if len(row) > 1 else 'N/A'}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Look for customers with mobile phone numbers (potential agents)
        print("\n🔍 Customers with Mobile Numbers (Potential Agents):")
        cursor.execute("""
            SELECT COUNT(*) FROM CUSTOMER 
            WHERE MOBILE_TEL IS NOT NULL 
              AND MOBILE_TEL != ''
              AND LENGTH(TRIM(MOBILE_TEL)) > 5
        """)
        mobile_customers = cursor.fetchone()[0]
        print(f"   Customers with mobile numbers: {mobile_customers:,}")
        
        if mobile_customers > 0:
            cursor.execute("""
                SELECT CUST_ID, FIRST_NAME, SURNAME, MOBILE_TEL, CUST_TYPE, EMPLOYER
                FROM CUSTOMER 
                WHERE MOBILE_TEL IS NOT NULL 
                  AND MOBILE_TEL != ''
                  AND LENGTH(TRIM(MOBILE_TEL)) > 5
                FETCH FIRST 10 ROWS ONLY
            """)
            rows = cursor.fetchall()
            print("   Sample customers with mobile numbers:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Mobile: {row[3]}, Type: {row[4]}")

if __name__ == "__main__":
    investigate_agents()