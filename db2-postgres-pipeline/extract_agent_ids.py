#!/usr/bin/env python3
"""
Extract unique customer IDs from found agents results
"""

import json

def extract_agent_ids():
    """Extract unique customer IDs from found agents results"""
    
    with open('found_agents_results.json', 'r') as f:
        data = json.load(f)
    
    # Extract unique customer IDs
    customer_ids = set()
    for agent in data['found_agents']:
        customer_ids.add(agent['customer_id'])
    
    # Sort the IDs
    sorted_ids = sorted(list(customer_ids))
    
    print("Unique Customer IDs from Found Agents:")
    print("=" * 50)
    print(f"Total unique agents: {len(sorted_ids)}")
    print()
    
    # Print as comma-separated list for SQL
    ids_str = ','.join(map(str, sorted_ids))
    print("SQL IN clause:")
    print(f"WHERE c.CUST_ID IN ({ids_str})")
    print()
    
    # Print as Python list
    print("Python list:")
    print(sorted_ids)
    
    return sorted_ids

if __name__ == "__main__":
    extract_agent_ids()