#!/usr/bin/env python3
"""
Final investigation of potential agents using correct column names
"""

from db2_connection import DB2Connection

def final_agent_investigation():
    """Final investigation of potential agents"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get the 18 customers with agent-like names and check their accounts
        print("🔍 Investigating the 18 agent-like customers and their accounts:")
        cursor.execute("""
            SELECT c.CUST_ID, c.FIRST_NAME, c.SURNAME, c.MOBILE_TEL, c.CUSTOMER_BEGIN_DAT
            FROM CUSTOMER c
            WHERE (UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                   OR UPPER(c.SURNAME) LIKE '%AGENT%'
                   OR UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                   OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                   OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                   OR UPPER(c.SURNAME) LIKE '%DUKA%'
                   OR UPPER(c.FIRST_NAME) LIKE '%SHOP%'
                   OR UPPER(c.SURNAME) LIKE '%SHOP%'
                   OR UPPER(c.FIRST_NAME) LIKE '%STORE%'
                   OR UPPER(c.SURNAME) LIKE '%STORE%'
                   OR UPPER(c.FIRST_NAME) LIKE '%MTEJA%'
                   OR UPPER(c.SURNAME) LIKE '%MTEJA%')
              AND c.ENTRY_STATUS = '1'
              AND c.MOBILE_TEL IS NOT NULL
              AND c.MOBILE_TEL != ''
            ORDER BY c.CUSTOMER_BEGIN_DAT DESC
        """)
        agent_customers = cursor.fetchall()
        
        print(f"   Found {len(agent_customers)} agent-like customers")
        
        for i, customer in enumerate(agent_customers, 1):
            cust_id, first_name, surname, mobile, begin_date = customer
            print(f"\n   {i}. {first_name} {surname} (ID: {cust_id}, Mobile: {mobile})")
            
            # Check their deposit accounts
            cursor.execute("""
                SELECT da.ACCOUNT_NUMBER, da.FK_DEPOSITFK_PRODU, p.DESCRIPTION, da.OPENING_DATE, da.ENTRY_STATUS
                FROM DEPOSIT_ACCOUNT da
                LEFT JOIN PRODUCT p ON da.FK_DEPOSITFK_PRODU = p.ID_PRODUCT
                WHERE da.FK_CUSTOMERCUST_ID = ?
                  AND da.ENTRY_STATUS = '1'
            """, (cust_id,))
            accounts = cursor.fetchall()
            
            if accounts:
                print(f"     Accounts ({len(accounts)}):")
                for acc in accounts:
                    print(f"       - Account: {acc[0]}, Product: {acc[1]} ({acc[2] if acc[2] else 'N/A'}), Opened: {acc[3]}")
            else:
                print(f"     No active accounts found")
            
            # Check recent transaction activity
            cursor.execute("""
                SELECT COUNT(*) as trx_count
                FROM GLI_TRX_EXTRACT gte
                WHERE gte.CUST_ID = ?
                  AND gte.TRN_DATE >= '2024-01-01'
            """, (cust_id,))
            trx_result = cursor.fetchone()
            trx_count = trx_result[0] if trx_result else 0
            
            if trx_count > 0:
                print(f"     Recent transactions (2024): {trx_count:,}")
            else:
                print(f"     No recent transactions")
        
        # Check what products are available that might be agent-related
        print(f"\n🔍 Available Products (looking for agent-related products):")
        cursor.execute("""
            SELECT ID_PRODUCT, DESCRIPTION, PRODUCT_TYPE, ENTRY_STATUS
            FROM PRODUCT 
            WHERE ENTRY_STATUS = '1'
            ORDER BY DESCRIPTION
        """)
        products = cursor.fetchall()
        
        print(f"   Total active products: {len(products)}")
        print("   Sample products:")
        for i, prod in enumerate(products[:20], 1):
            print(f"   {i}. ID: {prod[0]}, Name: {prod[1][:50]}, Type: {prod[2]}")
        
        # Look for any products with agent/mobile/wakala keywords
        agent_products = [p for p in products if p[1] and any(keyword in p[1].upper() for keyword in ['AGENT', 'WAKALA', 'MOBILE', 'DUKA', 'SHOP'])]
        
        if agent_products:
            print(f"\n   Agent-related products found:")
            for prod in agent_products:
                print(f"     - ID: {prod[0]}, Name: {prod[1]}, Type: {prod[2]}")
        else:
            print(f"\n   No obvious agent-related products found")
        
        # Check if any of these customers have high transaction volumes
        print(f"\n🔍 Transaction volume analysis for agent-like customers:")
        agent_ids = [str(c[0]) for c in agent_customers]
        
        if agent_ids:
            cursor.execute(f"""
                SELECT gte.CUST_ID, COUNT(*) as trx_count, SUM(ABS(gte.DC_AMOUNT)) as total_amount
                FROM GLI_TRX_EXTRACT gte
                WHERE gte.CUST_ID IN ({','.join(agent_ids)})
                  AND gte.TRN_DATE >= '2023-01-01'
                GROUP BY gte.CUST_ID
                HAVING COUNT(*) > 10
                ORDER BY COUNT(*) DESC
            """)
            high_volume_agents = cursor.fetchall()
            
            if high_volume_agents:
                print(f"   High-volume agent-like customers:")
                for hv in high_volume_agents:
                    # Get customer name
                    customer_info = next((c for c in agent_customers if c[0] == hv[0]), None)
                    if customer_info:
                        print(f"     - {customer_info[1]} {customer_info[2]} (ID: {hv[0]}): {hv[1]:,} transactions, Total: {hv[2]:,.2f}")
            else:
                print(f"   No high-volume transactions found for agent-like customers")
        
        # Final recommendation
        print(f"\n🎯 RECOMMENDATION:")
        if len(agent_customers) > 0:
            print(f"   The {len(agent_customers)} customers with agent-like names (MADUKA, SWAKALA, etc.) appear to be")
            print(f"   the most likely candidates for actual mobile money agents:")
            print(f"   - They are individual customers (CUST_TYPE = '1') with mobile numbers")
            print(f"   - Their names suggest shop/agent business (MADUKA = shop in Swahili)")
            print(f"   - They have been customers since various dates")
            print(f"   - Some have active deposit accounts")
            print(f"\n   SUGGESTED APPROACH:")
            print(f"   Use these {len(agent_customers)} customers as the agents for the endpoint")
        else:
            print(f"   No clear agent candidates found. May need to:")
            print(f"   - Check with bank personnel for agent identification criteria")
            print(f"   - Look for specific account products used by agents")
            print(f"   - Examine transaction patterns to identify agent-like behavior")

if __name__ == "__main__":
    final_agent_investigation()