#!/usr/bin/env python3
"""
Simple check of potential agents
"""

from db2_connection import DB2Connection

def simple_agent_check():
    """Simple check of potential agents"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get the agent-like customers
        print("🔍 Agent-like customers (MADUKA, SWAKALA, etc.):")
        cursor.execute("""
            SELECT CUST_ID, FIRST_NAME, SURNAME, MOBILE_TEL, CUSTOMER_BEGIN_DAT
            FROM CUSTOMER 
            WHERE (UPPER(SURNAME) LIKE '%DUKA%'
                   OR UPPER(SURNAME) LIKE '%WAKALA%')
              AND ENTRY_STATUS = '1'
              AND MOBILE_TEL IS NOT NULL
              AND MOBILE_TEL != ''
            ORDER BY CUSTOMER_BEGIN_DAT DESC
        """)
        agent_customers = cursor.fetchall()
        
        print(f"   Found {len(agent_customers)} potential agents:")
        for i, customer in enumerate(agent_customers, 1):
            print(f"   {i}. {customer[1]} {customer[2]} (ID: {customer[0]}, Mobile: {customer[3]})")
        
        # Check if they have accounts
        if agent_customers:
            agent_ids = [str(c[0]) for c in agent_customers]
            cursor.execute(f"""
                SELECT FK_CUSTOMERCUST_ID, COUNT(*) as account_count
                FROM DEPOSIT_ACCOUNT 
                WHERE FK_CUSTOMERCUST_ID IN ({','.join(agent_ids)})
                  AND ENTRY_STATUS = '1'
                GROUP BY FK_CUSTOMERCUST_ID
            """)
            account_counts = cursor.fetchall()
            
            print(f"\n   Account information:")
            for acc in account_counts:
                customer_info = next((c for c in agent_customers if c[0] == acc[0]), None)
                if customer_info:
                    print(f"   - {customer_info[1]} {customer_info[2]}: {acc[1]} active accounts")
        
        print(f"\n🎯 CONCLUSION:")
        print(f"   These {len(agent_customers)} customers with names like MADUKA (shop) and SWAKALA")
        print(f"   are the most likely candidates for mobile money agents in the database.")
        print(f"   They are individual customers with mobile numbers and Swahili names")
        print(f"   suggesting they operate shops or agent businesses.")

if __name__ == "__main__":
    simple_agent_check()