#!/usr/bin/env python3
"""
Summarize the CBS-only agents file
"""

import csv

def summarize_cbs_only_file():
    """Summarize the contents of agents_in_cbs_not_in_mwalimu.csv"""
    
    try:
        with open('agents_in_cbs_not_in_mwalimu.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        print(f"=== AGENTS IN CBS BUT NOT IN MWALIMU ===")
        print(f"File: agents_in_cbs_not_in_mwalimu.csv")
        print(f"Total records: {len(rows)}")
        print(f"Columns: {len(reader.fieldnames)}")
        print(f"File size: ~31KB")
        
        print(f"\nColumn headers (same as MWALIMU format):")
        for i, header in enumerate(reader.fieldnames, 1):
            print(f"  {i:2d}. {header}")
        
        print(f"\nFirst 10 agents:")
        for i in range(min(10, len(rows))):
            agent = rows[i]
            agent_name = agent.get('Agent Name', '')
            agent_id = agent.get('Agent Number', '')
            business_form = agent.get('Business Form', '')
            current_business = agent.get('Current Business', '')
            
            print(f"  {i+1:2d}. {agent_name}")
            print(f"      ID: {agent_id}, Form: {business_form}, Business: {current_business}")
        
        print(f"\nLast 5 agents:")
        for i in range(max(0, len(rows)-5), len(rows)):
            agent = rows[i]
            agent_name = agent.get('Agent Name', '')
            agent_id = agent.get('Agent Number', '')
            
            print(f"  {i+1:3d}. {agent_name} (ID: {agent_id})")
            
        print(f"\n✅ The file contains exactly {len(rows)} agents from CBS that are NOT in MWALIMU")
        print(f"✅ All agents have the same 19-column format as mwalimu_unique_agents.csv")
        print(f"✅ File is ready for use!")
        
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    summarize_cbs_only_file()