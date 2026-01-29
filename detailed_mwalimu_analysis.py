#!/usr/bin/env python3
"""
Detailed analysis of top duplicate records in MWALIMU-WAKALA.csv
"""

import csv
from collections import Counter, defaultdict

def analyze_top_duplicates():
    """Show detailed information for top duplicate records"""
    
    # Read all data and find duplicates
    all_agents = []
    agent_names = []
    
    with open('MWALIMU-WAKALA.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row_num, row in enumerate(reader, start=2):
            agent_name = row.get('Agent Name', '').strip()
            if agent_name:
                all_agents.append({
                    'row_number': row_num,
                    'agent_name': agent_name,
                    'agent_name_upper': agent_name.upper(),
                    'full_row': row
                })
                agent_names.append(agent_name.upper())
    
    # Find duplicates
    name_counts = Counter(agent_names)
    duplicates = {name: count for name, count in name_counts.items() if count > 1}
    
    # Group agents by name
    agents_by_name = defaultdict(list)
    for agent in all_agents:
        agents_by_name[agent['agent_name_upper']].append(agent)
    
    # Show top 3 duplicates in detail
    top_duplicates = sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:3]
    
    print("=== DETAILED ANALYSIS OF TOP 3 DUPLICATES ===\n")
    
    for agent_name, count in top_duplicates:
        print(f"AGENT: {agent_name} ({count} occurrences)")
        print("="*80)
        
        duplicate_agents = agents_by_name[agent_name]
        
        for i, agent in enumerate(duplicate_agents, 1):
            row = agent['full_row']
            print(f"\nOccurrence {i} (Row {agent['row_number']}):")
            
            # Show key fields
            key_fields = [
                'Agent Number', 'Terminal ID', 'Current Business', 'Business Form',
                'Region', 'District', 'Physical Location And Postal Address',
                'Gps Coordinates', 'contact', 'Tin'
            ]
            
            for field in key_fields:
                value = row.get(field, '').strip()
                if value:
                    if len(value) > 80:
                        value = value[:80] + "..."
                    print(f"  {field}: {value}")
                else:
                    print(f"  {field}: [EMPTY]")
        
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    analyze_top_duplicates()