#!/usr/bin/env python3
"""
Script to analyze duplicates in agent-from-cbs.csv by agent names
"""

import csv
from collections import Counter, defaultdict

def analyze_cbs_duplicates():
    """Analyze duplicates in agent-from-cbs.csv"""
    
    # Store all agent data
    all_agents = []
    agent_names = []
    
    try:
        with open('agent-from-cbs.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because of header
                agent_name = row.get('AGENTNAME', '').strip()
                agent_id = row.get('AGENTID', '').strip()
                
                if agent_name:
                    all_agents.append({
                        'row_number': row_num,
                        'agent_name': agent_name,
                        'agent_name_upper': agent_name.upper(),
                        'agent_id': agent_id,
                        'full_row': row
                    })
                    agent_names.append(agent_name.upper())
                    
    except Exception as e:
        print(f"Error reading agent-from-cbs.csv: {e}")
        return
    
    print("=== AGENT-FROM-CBS.CSV DUPLICATE ANALYSIS ===\n")
    
    # Basic statistics
    print(f"Total rows with agent names: {len(all_agents)}")
    print(f"Unique agent names: {len(set(agent_names))}")
    print(f"Duplicate entries: {len(agent_names) - len(set(agent_names))}\n")
    
    # Find duplicates
    name_counts = Counter(agent_names)
    duplicates = {name: count for name, count in name_counts.items() if count > 1}
    
    if duplicates:
        print(f"=== FOUND {len(duplicates)} AGENT NAMES WITH DUPLICATES ===\n")
        
        # Group agents by name for detailed analysis
        agents_by_name = defaultdict(list)
        for agent in all_agents:
            agents_by_name[agent['agent_name_upper']].append(agent)
        
        # Analyze each duplicate
        for duplicate_name, count in sorted(duplicates.items()):
            print(f"AGENT NAME: {duplicate_name}")
            print(f"Appears {count} times:")
            
            duplicate_agents = agents_by_name[duplicate_name]
            
            for i, agent in enumerate(duplicate_agents, 1):
                print(f"  {i}. Row {agent['row_number']}: ID='{agent['agent_id']}'")
                
                # Show some additional fields if available
                row = agent['full_row']
                additional_info = []
                
                # Check common fields that might help distinguish duplicates
                for field in ['BRANCHCODE', 'BRANCHNAME', 'REGION', 'DISTRICT', 'WARD']:
                    if field in row and row[field].strip():
                        additional_info.append(f"{field}={row[field].strip()}")
                
                if additional_info:
                    print(f"     Additional info: {', '.join(additional_info)}")
            
            print()  # Empty line between duplicates
    else:
        print("No duplicates found!")
    
    # Summary of duplicate patterns
    if duplicates:
        print("=== DUPLICATE SUMMARY ===")
        print(f"Total duplicate agent names: {len(duplicates)}")
        print(f"Total extra records due to duplicates: {sum(count - 1 for count in duplicates.values())}")
        
        # Show distribution of duplicate counts
        duplicate_distribution = Counter(duplicates.values())
        print("\nDuplicate distribution:")
        for count, frequency in sorted(duplicate_distribution.items()):
            print(f"  {frequency} agent(s) appear {count} times each")

if __name__ == "__main__":
    analyze_cbs_duplicates()