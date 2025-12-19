#!/usr/bin/env python3
"""
Find the specific agents from agents.json in the CUSTOMER table
"""

import sys
import os
import logging
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_agents_from_json():
    """Find the specific agents from agents.json in the CUSTOMER table"""
    
    # Read agents.json
    agents_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'agents.json')
    
    try:
        with open(agents_file, 'r') as f:
            agents_data = json.load(f)
        
        logger.info(f"📊 Loaded {len(agents_data)} agents from agents.json")
        
    except Exception as e:
        logger.error(f"❌ Error reading agents.json: {e}")
        return
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("FINDING AGENTS FROM JSON IN CUSTOMER TABLE")
            logger.info("=" * 80)
            
            found_agents = []
            not_found_agents = []
            
            for i, agent in enumerate(agents_data, 1):
                agent_name = agent.get('Name', '').strip()
                if not agent_name:
                    continue
                
                logger.info(f"\n🔍 {i:3d}. Searching for: {agent_name}")
                
                # Split name into parts for flexible searching
                name_parts = agent_name.split()
                
                # Try exact match first
                exact_query = """
                    SELECT 
                        c.CUST_ID,
                        TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                        c.CUST_TYPE,
                        c.ENTRY_STATUS,
                        c.MOBILE_TEL,
                        c.CUSTOMER_BEGIN_DAT
                    FROM CUSTOMER c
                    WHERE UPPER(TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, ''))) = UPPER(?)
                        AND c.ENTRY_STATUS = '1'
                """
                
                cursor.execute(exact_query, (agent_name,))
                exact_results = cursor.fetchall()
                
                if exact_results:
                    logger.info(f"     ✅ EXACT MATCH: {exact_results[0][1]} (ID: {exact_results[0][0]})")
                    found_agents.append({
                        'json_name': agent_name,
                        'customer_id': exact_results[0][0],
                        'customer_name': exact_results[0][1],
                        'cust_type': exact_results[0][2],
                        'mobile': exact_results[0][4],
                        'match_type': 'exact'
                    })
                    continue
                
                # Try partial match with all name parts
                if len(name_parts) >= 2:
                    partial_conditions = []
                    params = []
                    
                    for part in name_parts:
                        if len(part) > 2:  # Skip very short parts
                            partial_conditions.append("(UPPER(c.FIRST_NAME) LIKE UPPER(?) OR UPPER(c.MIDDLE_NAME) LIKE UPPER(?) OR UPPER(c.SURNAME) LIKE UPPER(?))")
                            params.extend([f'%{part}%', f'%{part}%', f'%{part}%'])
                    
                    if partial_conditions:
                        partial_query = f"""
                            SELECT 
                                c.CUST_ID,
                                TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                                c.CUST_TYPE,
                                c.ENTRY_STATUS,
                                c.MOBILE_TEL,
                                c.CUSTOMER_BEGIN_DAT
                            FROM CUSTOMER c
                            WHERE ({' AND '.join(partial_conditions)})
                                AND c.ENTRY_STATUS = '1'
                            ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                            FETCH FIRST 5 ROWS ONLY
                        """
                        
                        cursor.execute(partial_query, params)
                        partial_results = cursor.fetchall()
                        
                        if partial_results:
                            logger.info(f"     🔍 PARTIAL MATCHES:")
                            for result in partial_results:
                                logger.info(f"       - {result[1]} (ID: {result[0]}, Type: {result[2]})")
                            
                            # Take the first partial match
                            found_agents.append({
                                'json_name': agent_name,
                                'customer_id': partial_results[0][0],
                                'customer_name': partial_results[0][1],
                                'cust_type': partial_results[0][2],
                                'mobile': partial_results[0][4],
                                'match_type': 'partial'
                            })
                            continue
                
                # Not found
                logger.info(f"     ❌ NOT FOUND")
                not_found_agents.append(agent_name)
            
            # Summary
            logger.info("\n" + "=" * 80)
            logger.info("📊 SEARCH SUMMARY:")
            logger.info("=" * 80)
            logger.info(f"  Total agents in JSON: {len(agents_data)}")
            logger.info(f"  Found in CUSTOMER table: {len(found_agents)}")
            logger.info(f"  Not found: {len(not_found_agents)}")
            
            # Analyze found agents
            if found_agents:
                logger.info(f"\n📋 FOUND AGENTS ANALYSIS:")
                
                cust_type_counts = {}
                exact_matches = 0
                partial_matches = 0
                
                for agent in found_agents:
                    cust_type = agent['cust_type']
                    cust_type_counts[cust_type] = cust_type_counts.get(cust_type, 0) + 1
                    
                    if agent['match_type'] == 'exact':
                        exact_matches += 1
                    else:
                        partial_matches += 1
                
                logger.info(f"  Exact matches: {exact_matches}")
                logger.info(f"  Partial matches: {partial_matches}")
                logger.info(f"  Customer types:")
                for cust_type, count in cust_type_counts.items():
                    type_name = {'1': 'Individual', '2': 'Corporate', 'B': 'Business'}.get(cust_type, cust_type)
                    logger.info(f"    CUST_TYPE '{cust_type}' ({type_name}): {count}")
                
                # Show sample found agents
                logger.info(f"\n📋 SAMPLE FOUND AGENTS (first 10):")
                logger.info("    ID     | Customer Name                    | Type | JSON Name")
                logger.info("    " + "-" * 75)
                for agent in found_agents[:10]:
                    logger.info(f"    {agent['customer_id']:6} | {agent['customer_name']:32} | {agent['cust_type']:4} | {agent['json_name']}")
                
                # Generate customer IDs list for SQL query
                customer_ids = [str(agent['customer_id']) for agent in found_agents]
                logger.info(f"\n💡 CUSTOMER IDs FOR SQL QUERY:")
                logger.info(f"   WHERE c.CUST_ID IN ({','.join(customer_ids[:50])}...)")
                
                # Save results to file
                results_file = os.path.join(os.path.dirname(__file__), 'found_agents_results.json')
                with open(results_file, 'w') as f:
                    json.dump({
                        'found_agents': found_agents,
                        'not_found_agents': not_found_agents,
                        'summary': {
                            'total_json_agents': len(agents_data),
                            'found_count': len(found_agents),
                            'not_found_count': len(not_found_agents),
                            'exact_matches': exact_matches,
                            'partial_matches': partial_matches,
                            'cust_type_counts': cust_type_counts
                        }
                    }, f, indent=2)
                
                logger.info(f"\n💾 Results saved to: {results_file}")
            
            if not_found_agents:
                logger.info(f"\n❌ NOT FOUND AGENTS (first 10):")
                for agent_name in not_found_agents[:10]:
                    logger.info(f"    - {agent_name}")
            
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_agents_from_json()