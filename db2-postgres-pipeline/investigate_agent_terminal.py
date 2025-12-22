#!/usr/bin/env python3
"""
Deep investigation of AGENT_TERMINAL table structure and relationships
"""

from db2_connection import DB2Connection

def investigate_agent_terminal():
    """Deep dive into AGENT_TERMINAL structure"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get AGENT_TERMINAL structure
        print("🔍 AGENT_TERMINAL Table Structure:")
        cursor.execute("""
            SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS 
            FROM SYSCAT.COLUMNS 
            WHERE TABNAME = 'AGENT_TERMINAL' 
            ORDER BY COLNO
        """)
        columns = cursor.fetchall()
        for col in columns:
            print(f"   {col[0]}: {col[1]}({col[2]}) {'NULL' if col[4] == 'Y' else 'NOT NULL'}")
        
        # Check unique FK_AGENT_CUST_ID values
        print(f"\n🔍 Unique Customer IDs in AGENT_TERMINAL:")
        cursor.execute("SELECT COUNT(DISTINCT FK_AGENT_CUST_ID) FROM AGENT_TERMINAL")
        unique_customers = cursor.fetchone()[0]
        print(f"   Unique customer IDs: {unique_customers}")
        
        # Check relationship with CUSTOMER table
        print(f"\n🔍 AGENT_TERMINAL -> CUSTOMER Relationship:")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM AGENT_TERMINAL at 
            INNER JOIN CUSTOMER c ON at.FK_AGENT_CUST_ID = c.CUST_ID
        """)
        matched_customers = cursor.fetchone()[0]
        print(f"   Records with matching customers: {matched_customers}")
        
        # Sample AGENT_TERMINAL with CUSTOMER data
        print(f"\n🔍 Sample AGENT_TERMINAL with CUSTOMER data:")
        cursor.execute("""
            SELECT 
                at.USER_CODE,
                at.LOCATION,
                at.FK_AGENT_CUST_ID,
                c.FIRST_NAME,
                c.SURNAME,
                c.MOBILE_TEL,
                c.CUST_TYPE,
                at.INSERTION_DATE
            FROM AGENT_TERMINAL at 
            INNER JOIN CUSTOMER c ON at.FK_AGENT_CUST_ID = c.CUST_ID
            WHERE at.ENTRY_STATUS = '1'
            FETCH FIRST 10 ROWS ONLY
        """)
        rows = cursor.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"   {i}. USER_CODE: {row[0]}, LOCATION: {row[1][:30]}..., CUSTOMER: {row[3]} {row[4]}, MOBILE: {row[5]}")
        
        # Check for different USER_CODE patterns
        print(f"\n🔍 USER_CODE Patterns:")
        cursor.execute("""
            SELECT 
                SUBSTR(USER_CODE, 1, 3) as prefix,
                COUNT(*) as count
            FROM AGENT_TERMINAL 
            WHERE USER_CODE IS NOT NULL
            GROUP BY SUBSTR(USER_CODE, 1, 3)
            ORDER BY count DESC
        """)
        patterns = cursor.fetchall()
        for pattern in patterns[:10]:
            print(f"   {pattern[0]}: {pattern[1]} records")
        
        # Check CASH_TILL relationship
        print(f"\n🔍 CASH_TILL Investigation:")
        cursor.execute("SELECT COUNT(DISTINCT TILL_NO) FROM CASH_TILL")
        unique_tills = cursor.fetchone()[0]
        print(f"   Unique till numbers: {unique_tills}")
        
        cursor.execute("""
            SELECT TILL_NO, UNIT, FK_CURRENCYID_CURR, OPENING_BALANCE
            FROM CASH_TILL 
            FETCH FIRST 10 ROWS ONLY
        """)
        tills = cursor.fetchall()
        print("   Sample till data:")
        for i, till in enumerate(tills, 1):
            print(f"   {i}. TILL_NO: {till[0]}, UNIT: {till[1]}, CURRENCY: {till[2]}, BALANCE: {till[3]}")

if __name__ == "__main__":
    investigate_agent_terminal()