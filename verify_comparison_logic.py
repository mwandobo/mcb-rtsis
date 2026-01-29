#!/usr/bin/env python3
"""
Verify the comparison logic step by step
"""

import csv

def verify_comparison_logic():
    """Step-by-step verification of the comparison logic"""
    
    print("=== STEP-BY-STEP COMPARISON VERIFICATION ===\n")
    
    # Step 1: Read MWALIMU unique agents
    print("STEP 1: Reading MWALIMU unique agents...")
    mwalimu_agents = []
    mwalimu_names_set = set()
    
    with open('mwalimu_unique_agents.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row_num, row in enumerate(reader, 1):
            agent_name = row.get('Agent Name', '').strip()
            if agent_name:
                mwalimu_agents.append({
                    'row': row_num,
                    'name': agent_name,
                    'name_upper': agent_name.upper()
                })
                mwalimu_names_set.add(agent_name.upper())
    
    print(f"  - Total MWALIMU records: {len(mwalimu_agents)}")
    print(f"  - Unique MWALIMU names: {len(mwalimu_names_set)}")
    print(f"  - First 3 MWALIMU names: {[agent['name'] for agent in mwalimu_agents[:3]]}")
    
    # Step 2: Read CBS agents
    print(f"\nSTEP 2: Reading CBS agents...")
    cbs_agents = []
    cbs_names_set = set()
    
    with open('agent-from-cbs.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row_num, row in enumerate(reader, 1):
            agent_name = row.get('AGENTNAME', '').strip()
            if agent_name:
                cbs_agents.append({
                    'row': row_num,
                    'name': agent_name,
                    'name_upper': agent_name.upper(),
                    'id': row.get('AGENTID', '').strip()
                })
                cbs_names_set.add(agent_name.upper())
    
    print(f"  - Total CBS records: {len(cbs_agents)}")
    print(f"  - Unique CBS names: {len(cbs_names_set)}")
    print(f"  - First 3 CBS names: {[agent['name'] for agent in cbs_agents[:3]]}")
    
    # Step 3: Find overlaps
    print(f"\nSTEP 3: Finding overlaps...")
    overlap_names = mwalimu_names_set & cbs_names_set
    print(f"  - Agents in BOTH systems: {len(overlap_names)}")
    
    # Step 4: Find CBS-only agents
    print(f"\nSTEP 4: Finding CBS-only agents...")
    cbs_only_names = cbs_names_set - mwalimu_names_set
    print(f"  - Agents in CBS but NOT in MWALIMU: {len(cbs_only_names)}")
    
    # Step 5: Find MWALIMU-only agents
    print(f"\nSTEP 5: Finding MWALIMU-only agents...")
    mwalimu_only_names = mwalimu_names_set - cbs_names_set
    print(f"  - Agents in MWALIMU but NOT in CBS: {len(mwalimu_only_names)}")
    
    # Step 6: Verification
    print(f"\nSTEP 6: Mathematical verification...")
    print(f"  - CBS total: {len(cbs_names_set)}")
    print(f"  - Overlap + CBS-only: {len(overlap_names)} + {len(cbs_only_names)} = {len(overlap_names) + len(cbs_only_names)}")
    print(f"  - MWALIMU total: {len(mwalimu_names_set)}")
    print(f"  - Overlap + MWALIMU-only: {len(overlap_names)} + {len(mwalimu_only_names)} = {len(overlap_names) + len(mwalimu_only_names)}")
    
    # Step 7: Show specific examples
    print(f"\nSTEP 7: Specific examples...")
    
    print(f"\nFirst 5 overlapping agents:")
    overlap_list = list(overlap_names)[:5]
    for i, name in enumerate(overlap_list, 1):
        print(f"  {i}. {name}")
    
    print(f"\nFirst 5 CBS-only agents:")
    cbs_only_list = list(cbs_only_names)[:5]
    for i, name in enumerate(cbs_only_list, 1):
        print(f"  {i}. {name}")
    
    print(f"\nFirst 5 MWALIMU-only agents:")
    mwalimu_only_list = list(mwalimu_only_names)[:5]
    for i, name in enumerate(mwalimu_only_list, 1):
        print(f"  {i}. {name}")
    
    # Step 8: Cross-check with our previous results
    print(f"\nSTEP 8: Cross-check with previous analysis...")
    print("From our earlier name-only comparison script:")
    print("- We found 211 agents in MWALIMU but not in CBS")
    print("- We found 316 agents overlapping")
    print(f"Current results:")
    print(f"- MWALIMU-only: {len(mwalimu_only_names)}")
    print(f"- Overlap: {len(overlap_names)}")
    print(f"- CBS-only: {len(cbs_only_names)}")
    
    # Step 9: Manual spot check
    print(f"\nSTEP 9: Manual spot check...")
    print("Let's manually verify a few specific agents:")
    
    # Pick a few agents from each category and verify manually
    test_agents = [
        ("MABULA MUNYU MUTATA", "Should be in MWALIMU"),
        ("FRANK BAHATI MGENZI", "Should be in CBS only"),
        ("RYOBA MARWA CHACHA", "Should be in both")
    ]
    
    for test_name, expected in test_agents:
        test_name_upper = test_name.upper()
        in_mwalimu = test_name_upper in mwalimu_names_set
        in_cbs = test_name_upper in cbs_names_set
        
        print(f"  {test_name}:")
        print(f"    In MWALIMU: {in_mwalimu}")
        print(f"    In CBS: {in_cbs}")
        print(f"    Expected: {expected}")
        
        if expected == "Should be in MWALIMU" and not in_mwalimu:
            print(f"    ❌ ERROR: Expected in MWALIMU but not found!")
        elif expected == "Should be in CBS only" and (not in_cbs or in_mwalimu):
            print(f"    ❌ ERROR: Expected CBS-only but found in MWALIMU: {in_mwalimu}, CBS: {in_cbs}")
        elif expected == "Should be in both" and (not in_mwalimu or not in_cbs):
            print(f"    ❌ ERROR: Expected in both but MWALIMU: {in_mwalimu}, CBS: {in_cbs}")
        else:
            print(f"    ✅ Correct")

if __name__ == "__main__":
    verify_comparison_logic()