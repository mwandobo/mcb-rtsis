#!/usr/bin/env python3
"""
Analyze the key findings from the deep agent investigation
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_agent_findings():
    """Analyze key findings from agent investigation"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("ANALYZING KEY AGENT FINDINGS")
            logger.info("=" * 80)
            
            # Our known agent customer IDs
            known_agent_ids = [186,8536,8661,9368,13692,16765,22410,23958,25980,26587,26962,28651,32799,32992,34671,34967,37538,38208,38480,38971,38988,39122,39572,40248,41480,42338,42488,43415,45012,45117,45186,47027,47054,47283,48297,48877,50489,51611,51853,51893,52592,52733,52815,55606,56431,57175,59921,60087,60130,60175,60265,60611,60723,61305,61335,61927,62098,62310,62673]
            agent_ids_str = ','.join(map(str, known_agent_ids))
            
            # 1. Analyze AGENT_TERMINAL table - this looks very promising!
            logger.info("\n🎯 ANALYZING AGENT_TERMINAL TABLE (634 records)")
            logger.info("This table appears to link agents to terminals/POS devices")
            
            query_agent_terminal = """
                SELECT 
                    at.USER_CODE,
                    at.TERMINAL_TYPE,
                    at.LOCATION,
                    at.FK_AGENT_CUST_ID,
                    at.FK_AGENT_CHANNEL_ID,
                    at.ENTRY_STATUS,
                    c.FIRST_NAME,
                    c.MIDDLE_NAME,
                    c.SURNAME,
                    c.MOBILE_TEL
                FROM AGENT_TERMINAL at
                LEFT JOIN CUSTOMER c ON c.CUST_ID = at.FK_AGENT_CUST_ID
                WHERE at.ENTRY_STATUS = '1'
                ORDER BY at.FK_AGENT_CUST_ID
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query_agent_terminal)
            agent_terminals = cursor.fetchall()
            
            logger.info("Sample AGENT_TERMINAL records:")
            logger.info("  Terminal Code | Type | Location                    | Agent ID | Customer Name")
            logger.info("  " + "-" * 80)
            for user_code, term_type, location, agent_cust_id, channel_id, status, fname, mname, sname, mobile in agent_terminals:
                name = f"{fname or ''} {mname or ''} {sname or ''}".strip()
                location_display = location[:30] if location else ""
                logger.info(f"  {user_code:<12} | {term_type:<4} | {location_display:<30} | {agent_cust_id:<8} | {name}")
            
            # Check if any of our known agents are in AGENT_TERMINAL
            query_our_agents_in_terminal = f"""
                SELECT 
                    at.USER_CODE,
                    at.TERMINAL_TYPE,
                    at.LOCATION,
                    at.FK_AGENT_CUST_ID,
                    c.FIRST_NAME,
                    c.MIDDLE_NAME,
                    c.SURNAME,
                    c.MOBILE_TEL
                FROM AGENT_TERMINAL at
                LEFT JOIN CUSTOMER c ON c.CUST_ID = at.FK_AGENT_CUST_ID
                WHERE at.FK_AGENT_CUST_ID IN ({agent_ids_str})
                    AND at.ENTRY_STATUS = '1'
                ORDER BY at.FK_AGENT_CUST_ID
            """
            cursor.execute(query_our_agents_in_terminal)
            our_agents_terminals = cursor.fetchall()
            
            if our_agents_terminals:
                logger.info(f"\n✅ Found {len(our_agents_terminals)} of our known agents in AGENT_TERMINAL:")
                for user_code, term_type, location, agent_cust_id, fname, mname, sname, mobile in our_agents_terminals:
                    name = f"{fname or ''} {mname or ''} {sname or ''}".strip()
                    logger.info(f"  Agent {agent_cust_id}: {name} -> Terminal {user_code} ({term_type}) at {location}")
            else:
                logger.info("\n❌ None of our known agents found in AGENT_TERMINAL")
            
            # 2. Analyze the main AGENT table (only 2 records)
            logger.info("\n🎯 ANALYZING MAIN AGENT TABLE (2 records)")
            query_main_agents = """
                SELECT 
                    a.AGENT_ID,
                    a.FK_CUSTOMERCUST_ID,
                    a.TRANSACTION_ACCOUNT,
                    a.COMMISSION_ACCOUNT,
                    a.ENTRY_STATUS,
                    c.FIRST_NAME,
                    c.MIDDLE_NAME,
                    c.SURNAME,
                    c.MOBILE_TEL
                FROM AGENT a
                LEFT JOIN CUSTOMER c ON c.CUST_ID = a.FK_CUSTOMERCUST_ID
                WHERE a.ENTRY_STATUS = '1'
            """
            cursor.execute(query_main_agents)
            main_agents = cursor.fetchall()
            
            logger.info("Main AGENT table records:")
            for agent_id, cust_id, trans_acc, comm_acc, status, fname, mname, sname, mobile in main_agents:
                name = f"{fname or ''} {mname or ''} {sname or ''}".strip()
                logger.info(f"  Agent ID {agent_id}: Customer {cust_id} ({name})")
                logger.info(f"    Transaction Account: {trans_acc}")
                logger.info(f"    Commission Account: {comm_acc}")
            
            # 3. Look for transaction patterns of our known agents
            logger.info("\n🎯 ANALYZING TRANSACTION PATTERNS OF KNOWN AGENTS")
            
            # Check GLI_TRX_EXTRACT for agent transactions
            query_agent_transactions = f"""
                SELECT 
                    gte.CUST_ID,
                    c.FIRST_NAME,
                    c.MIDDLE_NAME,
                    c.SURNAME,
                    COUNT(*) as transaction_count,
                    MIN(gte.TRN_DATE) as first_transaction,
                    MAX(gte.TRN_DATE) as last_transaction,
                    SUM(CASE WHEN gte.DC_IND = 'D' THEN gte.DC_AMOUNT ELSE 0 END) as total_debits,
                    SUM(CASE WHEN gte.DC_IND = 'C' THEN gte.DC_AMOUNT ELSE 0 END) as total_credits
                FROM GLI_TRX_EXTRACT gte
                JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
                WHERE gte.CUST_ID IN ({agent_ids_str})
                    AND gte.TRN_DATE >= DATE('2024-01-01')
                GROUP BY gte.CUST_ID, c.FIRST_NAME, c.MIDDLE_NAME, c.SURNAME
                ORDER BY transaction_count DESC
                FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(query_agent_transactions)
            agent_transactions = cursor.fetchall()
            
            if agent_transactions:
                logger.info("Top 10 most active known agents (2024 transactions):")
                logger.info("  Customer ID | Name                    | Transactions | First      | Last       | Debits     | Credits")
                logger.info("  " + "-" * 100)
                for cust_id, fname, mname, sname, count, first, last, debits, credits in agent_transactions:
                    name = f"{fname or ''} {mname or ''} {sname or ''}".strip()[:20]
                    logger.info(f"  {cust_id:<11} | {name:<23} | {count:<12} | {first} | {last} | {debits:<10} | {credits}")
            else:
                logger.info("No recent transactions found for known agents")
            
            # 4. Check for mobile money related transactions
            logger.info("\n🎯 CHECKING MOBILE MONEY PATTERNS")
            
            query_mobile_money = f"""
                SELECT 
                    gte.CUST_ID,
                    c.FIRST_NAME,
                    c.MIDDLE_NAME,
                    c.SURNAME,
                    gl.EXTERNAL_GLACCOUNT,
                    gl.DESCRIPTION,
                    COUNT(*) as transaction_count,
                    SUM(gte.DC_AMOUNT) as total_amount
                FROM GLI_TRX_EXTRACT gte
                JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
                JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                WHERE gte.CUST_ID IN ({agent_ids_str})
                    AND gl.EXTERNAL_GLACCOUNT IN ('144000051','144000058','144000061','144000062','504080001')
                    AND gte.TRN_DATE >= DATE('2023-01-01')
                GROUP BY gte.CUST_ID, c.FIRST_NAME, c.MIDDLE_NAME, c.SURNAME, gl.EXTERNAL_GLACCOUNT, gl.DESCRIPTION
                ORDER BY transaction_count DESC
                FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(query_mobile_money)
            mobile_money_txns = cursor.fetchall()
            
            if mobile_money_txns:
                logger.info("Mobile money transactions by known agents:")
                logger.info("  Customer ID | Name                    | Account        | Description           | Count | Amount")
                logger.info("  " + "-" * 100)
                for cust_id, fname, mname, sname, account, desc, count, amount in mobile_money_txns:
                    name = f"{fname or ''} {mname or ''} {sname or ''}".strip()[:20]
                    desc_short = desc[:20] if desc else ""
                    logger.info(f"  {cust_id:<11} | {name:<23} | {account:<14} | {desc_short:<21} | {count:<5} | {amount}")
            else:
                logger.info("No mobile money transactions found for known agents")
            
            # 5. Check for commission/fee related accounts
            logger.info("\n🎯 CHECKING COMMISSION/FEE PATTERNS")
            
            query_commissions = f"""
                SELECT 
                    gte.CUST_ID,
                    c.FIRST_NAME,
                    c.MIDDLE_NAME,
                    c.SURNAME,
                    gl.EXTERNAL_GLACCOUNT,
                    gl.DESCRIPTION,
                    COUNT(*) as transaction_count,
                    SUM(CASE WHEN gte.DC_IND = 'C' THEN gte.DC_AMOUNT ELSE 0 END) as total_credits
                FROM GLI_TRX_EXTRACT gte
                JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
                JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                WHERE gte.CUST_ID IN ({agent_ids_str})
                    AND (gl.DESCRIPTION LIKE '%COMMISSION%' 
                         OR gl.DESCRIPTION LIKE '%FEE%'
                         OR gl.DESCRIPTION LIKE '%AGENT%'
                         OR gl.EXTERNAL_GLACCOUNT LIKE '%504%')
                    AND gte.TRN_DATE >= DATE('2023-01-01')
                GROUP BY gte.CUST_ID, c.FIRST_NAME, c.MIDDLE_NAME, c.SURNAME, gl.EXTERNAL_GLACCOUNT, gl.DESCRIPTION
                ORDER BY total_credits DESC
                FETCH FIRST 10 ROWS ONLY
            """
            cursor.execute(query_commissions)
            commission_txns = cursor.fetchall()
            
            if commission_txns:
                logger.info("Commission/fee transactions by known agents:")
                logger.info("  Customer ID | Name                    | Account        | Description           | Count | Credits")
                logger.info("  " + "-" * 100)
                for cust_id, fname, mname, sname, account, desc, count, credits in commission_txns:
                    name = f"{fname or ''} {mname or ''} {sname or ''}".strip()[:20]
                    desc_short = desc[:20] if desc else ""
                    logger.info(f"  {cust_id:<11} | {name:<23} | {account:<14} | {desc_short:<21} | {count:<5} | {credits}")
            else:
                logger.info("No commission/fee transactions found for known agents")
            
            # 6. Summary and recommendations
            logger.info("\n" + "=" * 80)
            logger.info("AGENT INVESTIGATION SUMMARY & RECOMMENDATIONS")
            logger.info("=" * 80)
            logger.info(f"✅ Confirmed {len(known_agent_ids)} agents from agents.json matching in CUSTOMER table")
            logger.info("✅ Found AGENT_TERMINAL table with 634 terminal records (potential agent terminals)")
            logger.info("✅ Found main AGENT table with 2 super agent records")
            logger.info("✅ Multiple tables reference our known agents (transactions, applications, etc.)")
            
            logger.info("\n📋 RECOMMENDED AGENT DATA SOURCES:")
            logger.info("1. PRIMARY: Use specific customer IDs from agents.json (59 unique agents)")
            logger.info("2. SECONDARY: AGENT_TERMINAL table for terminal/location information")
            logger.info("3. TERTIARY: Transaction patterns from GLI_TRX_EXTRACT for activity data")
            logger.info("4. SUPPLEMENTARY: Mobile money accounts for service information")
            
            logger.info("\n🔧 UPDATED AGENT QUERY STRATEGY:")
            logger.info("- Use the 59 specific customer IDs as the definitive agent list")
            logger.info("- Join with AGENT_TERMINAL for terminal information where available")
            logger.info("- Include transaction activity metrics")
            logger.info("- Add mobile money service indicators")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_agent_findings()