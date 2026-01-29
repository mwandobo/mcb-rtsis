#!/usr/bin/env python3
"""
Script to analyze duplicates in MWALIMU-WAKALA.csv by agent names
"""

import csv
from collections import Counter, defaultdict

def analyze_mwalimu_duplicates():
    """Analyze duplicates in MWALIMU-WAKALA.csv"""
    
    # Store all agent data
    all_agents = []
    agent_names = []
    
    try:
        with open('MWALIMU-WAKALA.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because of header
                agent_name = row.get('Agent Name', '').strip()
                agent_number = row.get('Agent Number', '').strip()
                
                if agent_name:
                    all_agents.append({
                        'row_number': row_num,
                        'agent_name': agent_name,
                        'agent_name_upper': agent_name.upper(),
                        'agent_number': agent_number,
                        'full_row': row
                    })
                    agent_names.append(agent_name.upper())
                    
    except Exception as e:
        print(f"Error reading MWALIMU-WAKALA.csv: {e}")
        return
    
    print("=== MWALIMU-WAKALA.CSV DUPLICATE ANALYSIS ===\n")
    
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
        
        # Analyze each duplicate (show first 10 for brevity)
        duplicate_items = list(sorted(duplicates.items()))
        for i, (duplicate_name, count) in enumerate(duplicate_items):
            if i >= 10:  # Limit to first 10 duplicates for readability
                print(f"... and {len(duplicate_items) - 10} more duplicates")
                break
                
            print(f"AGENT NAME: {duplicate_name}")
            print(f"Appears {count} times:")
            
            duplicate_agents = agents_by_name[duplicate_name]
            
            for j, agent in enumerate(duplicate_agents, 1):
                print(f"  {j}. Row {agent['row_number']}: Agent Number='{agent['agent_number']}'")
                
                # Show some additional distinguishing fields
                row = agent['full_row']
                additional_info = []
                
                # Check fields that might help distinguish duplicates
                for field in ['Terminal ID', 'Region', 'District', 'Physical Location And Postal Address', 'Current Business']:
                    if field in row and row[field].strip():
                        value = row[field].strip()
                        if len(value) > 50:  # Truncate long values
                            value = value[:50] + "..."
                        additional_info.append(f"{field}={value}")
                
                if additional_info:
                    print(f"     {', '.join(additional_info)}")
            
            print()  # Empty line between duplicates
    else:
        print("No duplicates found!")
    
    # Summary of duplicate patterns
    if duplicates:
        print("\n=== DUPLICATE SUMMARY ===")
        print(f"Total duplicate agent names: {len(duplicates)}")
        print(f"Total extra records due to duplicates: {sum(count - 1 for count in duplicates.values())}")
        
        # Show distribution of duplicate counts
        duplicate_distribution = Counter(duplicates.values())
        print("\nDuplicate distribution:")
        for count, frequency in sorted(duplicate_distribution.items()):
            print(f"  {frequency} agent(s) appear {count} times each")
        
        # Show top duplicates by frequency
        print(f"\nTop duplicates by frequency:")
        top_duplicates = sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:5]
        for name, count in top_duplicates:
            print(f"  {name}: {count} times")

if __name__ == "__main__":
    analyze_mwalimu_duplicates()