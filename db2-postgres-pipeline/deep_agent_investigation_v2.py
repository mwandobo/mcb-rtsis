#!/usr/bin/env python3
"""
Deep Agent Investigation V2 - Comprehensive Database Analysis
"""

from db2_connection import DB2Connection
import logging
import json

def investigate_agent_data():
    logging.basicConfig(level=logging.INFO)
    db2_conn = DB2Connection()
    
    investigation_results = {
        "agent_related_tables": [],
        "customer_analysis": {},
        "terminal_analysis": {},
        "transaction_patterns": {},
        "mobile_money_data": {},
        "account_patterns": {},
        "location_data": {}
    }
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            print("🔍 DEEP AGENT INVESTIGATION - COMPREHENSIVE DATABASE ANALYSIS")
            print("=" * 80)
            
            # 1. Find all tables with 'AGENT' in name or description
            print("\n1️⃣ SEARCHING FOR AGENT-RELATED TABLES...")
            agent_tables_query = """
            SELECT TABNAME, REMARKS, CARD 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = 'PROFITS' 
                AND (UPPER(TABNAME) LIKE '%AGENT%' 
                     OR UPPER(TABNAME) LIKE '%TERMINAL%'
                     OR UPPER(TABNAME) LIKE '%MOBILE%'
                     OR UPPER(TABNAME) LIKE '%WAKALA%'
                     OR UPPER(TABNAME) LIKE '%DUKA%'
                     OR UPPER(REMARKS) LIKE '%AGENT%')
            ORDER BY TABNAME
            """
            
            cursor.execute(agent_tables_query)
            agent_tables = cursor.fetchall()
            
            for table in agent_tables:
                table_name = table[0]
                remarks = table[1] if table[1] else "No description"
                row_count = table[2] if table[2] else 0
                
                print(f"  📋 {table_name}: {remarks} ({row_count} rows)")
                investigation_results["agent_related_tables"].append({
                    "name": table_name,
                    "description": remarks,
                    "row_count": row_count
                })
                
                # Get sample data from each agent table
                try:
                    sample_query = f"SELECT * FROM {table_name} FETCH FIRST 3 ROWS ONLY"
                    cursor.execute(sample_query)
                    sample_data = cursor.fetchall()
                    
                    if sample_data:
                        print(f"    📊 Sample data: {len(sample_data)} rows")
                        # Get column names
                        col_query = f"""
                        SELECT COLNAME, TYPENAME, LENGTH 
                        FROM SYSCAT.COLUMNS 
                        WHERE TABSCHEMA = 'PROFITS' AND TABNAME = '{table_name}'
                        ORDER BY COLNO
                        """
                        cursor.execute(col_query)
                        columns = cursor.fetchall()
                        col_names = [col[0] for col in columns]
                        
                        for i, row in enumerate(sample_data):
                            print(f"      Row {i+1}: {dict(zip(col_names[:len(row)], row))}")
                    
                except Exception as e:
                    print(f"    ⚠️ Error getting sample data: {e}")
            
            # 2. Analyze CUSTOMER table for agent patterns
            print("\n2️⃣ ANALYZING CUSTOMER TABLE FOR AGENT PATTERNS...")
            
            # Customer type distribution
            cust_type_query = """
            SELECT CUST_TYPE, COUNT(*) as count,
                   COUNT(CASE WHEN MOBILE_TEL IS NOT NULL AND MOBILE_TEL != '' THEN 1 END) as with_mobile
            FROM CUSTOMER 
            WHERE ENTRY_STATUS = '1'
            GROUP BY CUST_TYPE 
            ORDER BY count DESC
            """
            cursor.execute(cust_type_query)
            cust_types = cursor.fetchall()
            
            print("  📊 Customer Type Distribution:")
            for ctype in cust_types:
                print(f"    Type {ctype[0]}: {ctype[1]} customers ({ctype[2]} with mobile)")
                investigation_results["customer_analysis"][f"type_{ctype[0]}"] = {
                    "total": ctype[1],
                    "with_mobile": ctype[2]
                }
            
            # Look for customers with agent-like names
            agent_name_patterns = [
                "WAKALA", "DUKA", "MADUKA", "AGENT", "SHOP", "STORE", 
                "BUSINESS", "ENTERPRISE", "COMPANY", "LIMITED", "LTD"
            ]
            
            print("\n  🔍 Searching for agent-like customer names...")
            for pattern in agent_name_patterns:
                name_query = f"""
                SELECT COUNT(*) as count,
                       COUNT(CASE WHEN MOBILE_TEL IS NOT NULL THEN 1 END) as with_mobile
                FROM CUSTOMER 
                WHERE ENTRY_STATUS = '1' 
                    AND (UPPER(FIRST_NAME) LIKE '%{pattern}%' 
                         OR UPPER(SURNAME) LIKE '%{pattern}%'
                         OR UPPER(MIDDLE_NAME) LIKE '%{pattern}%')
                """
                cursor.execute(name_query)
                result = cursor.fetchone()
                if result[0] > 0:
                    print(f"    {pattern}: {result[0]} customers ({result[1]} with mobile)")
            
            # 3. Analyze AGENT_TERMINAL table in detail
            print("\n3️⃣ ANALYZING AGENT_TERMINAL TABLE...")
            
            terminal_analysis_queries = [
                ("Total terminals", "SELECT COUNT(*) FROM AGENT_TERMINAL"),
                ("Active terminals", "SELECT COUNT(*) FROM AGENT_TERMINAL WHERE ENTRY_STATUS = '1'"),
                ("Terminals with locations", "SELECT COUNT(*) FROM AGENT_TERMINAL WHERE LOCATION IS NOT NULL AND LOCATION != ''"),
                ("Unique customer IDs", "SELECT COUNT(DISTINCT FK_AGENT_CUST_ID) FROM AGENT_TERMINAL WHERE FK_AGENT_CUST_ID IS NOT NULL"),
                ("Terminals by status", """
                    SELECT ENTRY_STATUS, COUNT(*) 
                    FROM AGENT_TERMINAL 
                    GROUP BY ENTRY_STATUS
                """)
            ]
            
            for desc, query in terminal_analysis_queries:
                try:
                    cursor.execute(query)
                    if "GROUP BY" in query:
                        results = cursor.fetchall()
                        print(f"  📊 {desc}:")
                        for result in results:
                            print(f"    Status {result[0]}: {result[1]} terminals")
                    else:
                        result = cursor.fetchone()
                        print(f"  📊 {desc}: {result[0]}")
                except Exception as e:
                    print(f"  ⚠️ Error in {desc}: {e}")
            
            # Sample terminal data with customer info
            print("\n  📋 Sample terminal data with customer information:")
            terminal_sample_query = """
            SELECT at.USER_CODE, at.LOCATION, at.ENTRY_STATUS,
                   c.FIRST_NAME, c.SURNAME, c.CUST_TYPE, c.MOBILE_TEL
            FROM AGENT_TERMINAL at
            LEFT JOIN CUSTOMER c ON c.CUST_ID = at.FK_AGENT_CUST_ID
            WHERE at.ENTRY_STATUS = '1'
            FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(terminal_sample_query)
            terminal_samples = cursor.fetchall()
            
            for i, sample in enumerate(terminal_samples):
                print(f"    {i+1}. Terminal: {sample[0]}, Location: {sample[1]}")
                print(f"       Customer: {sample[3]} {sample[4]} (Type: {sample[5]}, Mobile: {sample[6]})")
            
            # 4. Look for transaction patterns that indicate agents
            print("\n4️⃣ ANALYZING TRANSACTION PATTERNS...")
            
            # Check GLI_TRX_EXTRACT for agent activity
            transaction_queries = [
                ("Total transactions", "SELECT COUNT(*) FROM GLI_TRX_EXTRACT"),
                ("Transactions with customer IDs", "SELECT COUNT(*) FROM GLI_TRX_EXTRACT WHERE CUST_ID IS NOT NULL"),
                ("Unique customers in transactions", "SELECT COUNT(DISTINCT CUST_ID) FROM GLI_TRX_EXTRACT WHERE CUST_ID IS NOT NULL"),
                ("High-volume customers", """
                    SELECT CUST_ID, COUNT(*) as txn_count
                    FROM GLI_TRX_EXTRACT 
                    WHERE CUST_ID IS NOT NULL
                    GROUP BY CUST_ID 
                    HAVING COUNT(*) > 100
                    ORDER BY txn_count DESC
                    FETCH FIRST 10 ROWS ONLY
                """)
            ]
            
            for desc, query in transaction_queries:
                try:
                    cursor.execute(query)
                    if "GROUP BY" in query:
                        results = cursor.fetchall()
                        print(f"  📊 {desc}:")
                        for result in results:
                            print(f"    Customer {result[0]}: {result[1]} transactions")
                    else:
                        result = cursor.fetchone()
                        print(f"  📊 {desc}: {result[0]}")
                except Exception as e:
                    print(f"  ⚠️ Error in {desc}: {e}")
            
            # 5. Check for mobile money related tables
            print("\n5️⃣ SEARCHING FOR MOBILE MONEY TABLES...")
            
            mobile_tables_query = """
            SELECT TABNAME, REMARKS, CARD 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = 'PROFITS' 
                AND (UPPER(TABNAME) LIKE '%MOBILE%' 
                     OR UPPER(TABNAME) LIKE '%MPESA%'
                     OR UPPER(TABNAME) LIKE '%AIRTEL%'
                     OR UPPER(TABNAME) LIKE '%TIGO%'
                     OR UPPER(TABNAME) LIKE '%HALO%'
                     OR UPPER(TABNAME) LIKE '%FLOAT%')
            ORDER BY TABNAME
            """
            
            cursor.execute(mobile_tables_query)
            mobile_tables = cursor.fetchall()
            
            for table in mobile_tables:
                table_name = table[0]
                remarks = table[1] if table[1] else "No description"
                row_count = table[2] if table[2] else 0
                
                print(f"  📱 {table_name}: {remarks} ({row_count} rows)")
            
            # 6. Look for account patterns that suggest agents
            print("\n6️⃣ ANALYZING ACCOUNT PATTERNS...")
            
            # Check GLG_ACCOUNT for agent-related accounts
            account_query = """
            SELECT EXTERNAL_GLACCOUNT, ACCOUNT_DESCRIPTION, COUNT(*) as usage_count
            FROM GLG_ACCOUNT ga
            LEFT JOIN GLI_TRX_EXTRACT gte ON gte.FK_GLG_ACCOUNTACCO = ga.ACCOUNT_ID
            WHERE UPPER(ga.ACCOUNT_DESCRIPTION) LIKE '%AGENT%'
               OR UPPER(ga.ACCOUNT_DESCRIPTION) LIKE '%MOBILE%'
               OR UPPER(ga.ACCOUNT_DESCRIPTION) LIKE '%FLOAT%'
               OR ga.EXTERNAL_GLACCOUNT LIKE '144%'
            GROUP BY EXTERNAL_GLACCOUNT, ACCOUNT_DESCRIPTION
            ORDER BY usage_count DESC
            FETCH FIRST 20 ROWS ONLY
            """
            
            try:
                cursor.execute(account_query)
                accounts = cursor.fetchall()
                
                print("  💳 Agent-related accounts:")
                for account in accounts:
                    print(f"    {account[0]}: {account[1]} ({account[2]} transactions)")
            except Exception as e:
                print(f"  ⚠️ Error analyzing accounts: {e}")
            
            # 7. Check specific customer IDs from agents.json
            print("\n7️⃣ ANALYZING SPECIFIC AGENT CUSTOMER IDs...")
            
            agent_ids = [186,8536,8661,9368,13692,16765,22410,23958,25980,26587,26962,28651,32799,32992,34671,34967,37538,38208,38480,38971,38988,39122,39572,40248,41480,42338,42488,43415,45012,45117,45186,47027,47054,47283,48297,48877,50489,51611,51853,51893,52592,52733,52815,55606,56431,57175,59921,60087,60130,60175,60265,60611,60723,61305,61335,61927,62098,62310,62673]
            
            # Check how many of these exist in CUSTOMER table
            agent_check_query = f"""
            SELECT COUNT(*) as total_found,
                   COUNT(CASE WHEN ENTRY_STATUS = '1' THEN 1 END) as active,
                   COUNT(CASE WHEN MOBILE_TEL IS NOT NULL AND MOBILE_TEL != '' THEN 1 END) as with_mobile,
                   COUNT(CASE WHEN CUST_TYPE = '1' THEN 1 END) as individual,
                   COUNT(CASE WHEN CUST_TYPE = '2' THEN 1 END) as corporate
            FROM CUSTOMER 
            WHERE CUST_ID IN ({','.join(map(str, agent_ids))})
            """
            
            cursor.execute(agent_check_query)
            agent_stats = cursor.fetchone()
            
            print(f"  📊 Agent IDs from agents.json:")
            print(f"    Total found in CUSTOMER table: {agent_stats[0]}")
            print(f"    Active customers: {agent_stats[1]}")
            print(f"    With mobile numbers: {agent_stats[2]}")
            print(f"    Individual customers: {agent_stats[3]}")
            print(f"    Corporate customers: {agent_stats[4]}")
            
            # Check terminal associations
            terminal_check_query = f"""
            SELECT COUNT(*) as agents_with_terminals,
                   COUNT(CASE WHEN at.ENTRY_STATUS = '1' THEN 1 END) as active_terminals
            FROM CUSTOMER c
            LEFT JOIN AGENT_TERMINAL at ON at.FK_AGENT_CUST_ID = c.CUST_ID
            WHERE c.CUST_ID IN ({','.join(map(str, agent_ids))})
                AND at.FK_AGENT_CUST_ID IS NOT NULL
            """
            
            cursor.execute(terminal_check_query)
            terminal_stats = cursor.fetchone()
            
            print(f"    Agents with terminals: {terminal_stats[0]}")
            print(f"    Active terminals: {terminal_stats[1]}")
            
            # Sample agent data
            print("\n  📋 Sample agent customer data:")
            sample_agents_query = f"""
            SELECT c.CUST_ID, c.FIRST_NAME, c.SURNAME, c.CUST_TYPE, c.MOBILE_TEL,
                   at.USER_CODE, at.LOCATION
            FROM CUSTOMER c
            LEFT JOIN AGENT_TERMINAL at ON at.FK_AGENT_CUST_ID = c.CUST_ID AND at.ENTRY_STATUS = '1'
            WHERE c.CUST_ID IN ({','.join(map(str, agent_ids[:10]))})
            ORDER BY c.CUST_ID
            """
            
            cursor.execute(sample_agents_query)
            sample_agents = cursor.fetchall()
            
            for agent in sample_agents:
                print(f"    ID: {agent[0]}, Name: {agent[1]} {agent[2]}, Type: {agent[3]}")
                print(f"        Mobile: {agent[4]}, Terminal: {agent[5]}, Location: {agent[6]}")
            
            print("\n" + "=" * 80)
            print("🎯 INVESTIGATION COMPLETE!")
            print("=" * 80)
            
            # Save results to file
            with open('agent_investigation_results_v2.json', 'w') as f:
                json.dump(investigation_results, f, indent=2, default=str)
            
            print("📄 Results saved to: agent_investigation_results_v2.json")
            
    except Exception as e:
        print(f"❌ Investigation error: {e}")

if __name__ == "__main__":
    investigate_agent_data()