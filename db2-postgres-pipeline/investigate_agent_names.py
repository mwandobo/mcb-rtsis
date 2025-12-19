#!/usr/bin/env python3
"""
Investigate customers with agent-like names and look for agent account products
"""

from db2_connection import DB2Connection

def investigate_agent_names():
    """Investigate customers with agent-like names"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all customers with agent-like names
        print("🔍 All customers with agent-like names:")
        cursor.execute("""
            SELECT CUST_ID, FIRST_NAME, SURNAME, MOBILE_TEL, CUST_TYPE, CUSTOMER_BEGIN_DAT, EMPLOYER
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
            ORDER BY CUSTOMER_BEGIN_DAT DESC
        """)
        agent_customers = cursor.fetchall()
        
        print(f"   Total agent-like customers: {len(agent_customers)}")
        print("   All agent-like customers:")
        for i, row in enumerate(agent_customers, 1):
            print(f"   {i}. ID: {row[0]}, Name: {row[1]} {row[2]}, Mobile: {row[3]}, Type: {row[4]}, Employer: {row[6][:30] if row[6] else 'N/A'}")
        
        # Look for account products that might indicate agent services
        print(f"\n🔍 Looking for agent-related account products:")
        try:
            # Check DEPOSIT_ACCOUNT table for agent accounts
            cursor.execute("SELECT COUNT(*) FROM DEPOSIT_ACCOUNT")
            deposit_count = cursor.fetchone()[0]
            print(f"   DEPOSIT_ACCOUNT records: {deposit_count:,}")
            
            if deposit_count > 0:
                cursor.execute("""
                    SELECT ACCOUNT_NUMBER, CUST_ID, PRODUCT_ID, ACCOUNT_STATUS
                    FROM DEPOSIT_ACCOUNT 
                    WHERE ACCOUNT_STATUS = '1'
                    FETCH FIRST 10 ROWS ONLY
                """)
                accounts = cursor.fetchall()
                print("   Sample deposit accounts:")
                for acc in accounts[:5]:
                    print(f"     Account: {acc[0]}, Customer: {acc[1]}, Product: {acc[2]}, Status: {acc[3]}")
                    
        except Exception as e:
            print(f"   DEPOSIT_ACCOUNT error: {e}")
        
        # Check for product definitions
        try:
            cursor.execute("SELECT COUNT(*) FROM PRODUCT")
            product_count = cursor.fetchone()[0]
            print(f"\n   PRODUCT records: {product_count:,}")
            
            if product_count > 0:
                cursor.execute("""
                    SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_TYPE
                    FROM PRODUCT 
                    WHERE UPPER(PRODUCT_NAME) LIKE '%AGENT%'
                       OR UPPER(PRODUCT_NAME) LIKE '%WAKALA%'
                       OR UPPER(PRODUCT_NAME) LIKE '%MOBILE%'
                    FETCH FIRST 10 ROWS ONLY
                """)
                agent_products = cursor.fetchall()
                if agent_products:
                    print("   Agent-related products:")
                    for prod in agent_products:
                        print(f"     ID: {prod[0]}, Name: {prod[1]}, Type: {prod[2]}")
                else:
                    print("   No agent-related products found")
                    
                    # Show sample products
                    cursor.execute("SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_TYPE FROM PRODUCT FETCH FIRST 10 ROWS ONLY")
                    sample_products = cursor.fetchall()
                    print("   Sample products:")
                    for prod in sample_products:
                        print(f"     ID: {prod[0]}, Name: {prod[1][:50]}, Type: {prod[2]}")
                        
        except Exception as e:
            print(f"   PRODUCT error: {e}")
        
        # Check if any of the agent-like customers have specific account types
        if agent_customers:
            print(f"\n🔍 Checking accounts for agent-like customers:")
            agent_ids = [str(row[0]) for row in agent_customers[:10]]  # Check first 10
            
            try:
                cursor.execute(f"""
                    SELECT da.CUST_ID, da.ACCOUNT_NUMBER, da.PRODUCT_ID, p.PRODUCT_NAME
                    FROM DEPOSIT_ACCOUNT da
                    LEFT JOIN PRODUCT p ON da.PRODUCT_ID = p.PRODUCT_ID
                    WHERE da.CUST_ID IN ({','.join(agent_ids)})
                      AND da.ACCOUNT_STATUS = '1'
                """)
                agent_accounts = cursor.fetchall()
                
                if agent_accounts:
                    print("   Accounts for agent-like customers:")
                    for acc in agent_accounts:
                        print(f"     Customer: {acc[0]}, Account: {acc[1]}, Product: {acc[2]} - {acc[3][:50] if acc[3] else 'N/A'}")
                else:
                    print("   No active accounts found for agent-like customers")
                    
            except Exception as e:
                print(f"   Account lookup error: {e}")
        
        # Look for any transaction patterns that might indicate agent activity
        print(f"\n🔍 Looking for transaction patterns:")
        try:
            cursor.execute("SELECT COUNT(*) FROM GLI_TRX_EXTRACT WHERE TRN_DATE >= '2024-01-01'")
            recent_trx = cursor.fetchone()[0]
            print(f"   Recent transactions (2024): {recent_trx:,}")
            
            # Check for high-volume customers (potential agents)
            if recent_trx > 0:
                cursor.execute("""
                    SELECT gte.FK_CUSTOMERCUST_ID, COUNT(*) as trx_count
                    FROM GLI_TRX_EXTRACT gte
                    WHERE gte.TRN_DATE >= '2024-01-01'
                      AND gte.FK_CUSTOMERCUST_ID IS NOT NULL
                    GROUP BY gte.FK_CUSTOMERCUST_ID
                    HAVING COUNT(*) > 100
                    ORDER BY COUNT(*) DESC
                    FETCH FIRST 10 ROWS ONLY
                """)
                high_volume = cursor.fetchall()
                
                if high_volume:
                    print("   High-volume customers (potential agents):")
                    for hv in high_volume:
                        # Get customer details
                        cursor.execute(f"SELECT FIRST_NAME, SURNAME, MOBILE_TEL FROM CUSTOMER WHERE CUST_ID = {hv[0]}")
                        cust_details = cursor.fetchone()
                        if cust_details:
                            print(f"     Customer: {hv[0]} ({cust_details[0]} {cust_details[1]}, {cust_details[2]}), Transactions: {hv[1]:,}")
                        else:
                            print(f"     Customer: {hv[0]}, Transactions: {hv[1]:,}")
                            
        except Exception as e:
            print(f"   Transaction analysis error: {e}")

if __name__ == "__main__":
    investigate_agent_names()