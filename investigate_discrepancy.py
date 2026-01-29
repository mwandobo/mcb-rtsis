#!/usr/bin/env python3
"""
Investigate the discrepancy in agent counts
"""

import csv
from collections import Counter

def investigate_discrepancy():
    """Investigate why we got 411 agents instead of expected fewer"""
    
    print("=== DETAILED INVESTIGATION ===\n")
    
    # Read MWALIMU unique agents
    mwalimu_agents = set()
    mwalimu_agents_list = []
    
    with open('mwalimu_unique_agents.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            agent_name = row.get('Agent Name', '').strip()
            if agent_name:
                mwalimu_agents.add(agent_name.upper())
                mwalimu_agents_list.append(agent_name)
    
    print(f"MWALIMU unique agents: {len(mwalimu_agents)}")
    
    # Read CBS agents (with duplicates)
    cbs_agents_all = []
    cbs_agents_unique = set()
    
    with open('agent-from-cbs.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            agent_name = row.get('AGENTNAME', '').strip()
            if agent_name:
                cbs_agents_all.append(agent_name)
                cbs_agents_unique.add(agent_name.upper())
    
    print(f"CBS total records: {len(cbs_agents_all)}")
    print(f"CBS unique agents: {len(cbs_agents_unique)}")
    
    # Find overlap
    overlap = mwalimu_agents & cbs_agents_unique
    print(f"Overlap (agents in both): {len(overlap)}")
    
    # Find CBS-only agents
    cbs_only = cbs_agents_unique - mwalimu_agents
    print(f"CBS-only agents: {len(cbs_only)}")
    
    # Find MWALIMU-only agents  
    mwalimu_only = mwalimu_agents - cbs_agents_unique
    print(f"MWALIMU-only agents: {len(mwalimu_only)}")
    
    print(f"\nVerification: {len(overlap)} + {len(cbs_only)} = {len(overlap) + len(cbs_only)} (should equal {len(cbs_agents_unique)})")
    print(f"Verification: {len(overlap)} + {len(mwalimu_only)} = {len(overlap) + len(mwalimu_only)} (should equal {len(mwalimu_agents)})")
    
    # Show some examples of CBS-only agents
    print(f"\n=== SAMPLE CBS-ONLY AGENTS ===")
    cbs_only_list = list(cbs_only)[:10]
    for i, agent in enumerate(cbs_only_list, 1):
        print(f"{i}. {agent}")
    
    # Show some examples of MWALIMU-only agents
    print(f"\n=== SAMPLE MWALIMU-ONLY AGENTS ===")
    mwalimu_only_list = list(mwalimu_only)[:10]
    for i, agent in enumerate(mwalimu_only_list, 1):
        print(f"{i}. {agent}")
    
    # Show some examples of overlapping agents
    print(f"\n=== SAMPLE OVERLAPPING AGENTS ===")
    overlap_list = list(overlap)[:10]
    for i, agent in enumerate(overlap_list, 1):
        print(f"{i}. {agent}")
    
    # Check our previous comparison results
    print(f"\n=== COMPARISON WITH PREVIOUS RESULTS ===")
    print("Previous analysis (name-only comparison):")
    print("- MWALIMU-WAKALA.csv: 527 unique names")
    print("- agent-from-cbs.csv: 726 unique names")
    print("- Missing from CBS: 211 agents")
    print("- This suggested overlap of: 527 - 211 = 316 agents")
    print()
    print("Current analysis:")
    print(f"- MWALIMU unique: {len(mwalimu_agents)}")
    print(f"- CBS unique: {len(cbs_agents_unique)}")
    print(f"- Overlap: {len(overlap)}")
    print(f"- CBS-only: {len(cbs_only)}")
    
    # The discrepancy might be due to the original MWALIMU-WAKALA.csv vs mwalimu_unique_agents.csv
    # Let's check the original file
    print(f"\n=== CHECKING ORIGINAL MWALIMU-WAKALA.CSV ===")
    original_mwalimu_agents = set()
    
    try:
        with open('MWALIMU-WAKALA.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                agent_name = row.get('Agent Name', '').strip()
                if agent_name:
                    original_mwalimu_agents.add(agent_name.upper())
        
        print(f"Original MWALIMU-WAKALA.csv unique names: {len(original_mwalimu_agents)}")
        
        # Compare with CBS using original MWALIMU
        original_overlap = original_mwalimu_agents & cbs_agents_unique
        original_cbs_only = cbs_agents_unique - original_mwalimu_agents
        
        print(f"Overlap with original MWALIMU: {len(original_overlap)}")
        print(f"CBS-only using original MWALIMU: {len(original_cbs_only)}")
        
        # This should match our previous analysis
        print(f"Expected CBS-only from previous analysis: {len(cbs_agents_unique)} - {len(original_overlap)} = {len(cbs_agents_unique) - len(original_overlap)}")
        
    except Exception as e:
        print(f"Error reading original file: {e}")

if __name__ == "__main__":
    investigate_discrepancy()