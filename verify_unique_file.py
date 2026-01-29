#!/usr/bin/env python3
"""
Verify the unique agents CSV file
"""

import csv

def verify_unique_file():
    """Verify the unique agents file"""
    
    try:
        with open('mwalimu_unique_agents.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        print(f"Total rows in unique agents file: {len(rows)}")
        print(f"Columns: {list(rows[0].keys())}")
        print(f"\nFirst 5 agent names:")
        
        for i in range(min(5, len(rows))):
            agent_name = rows[i].get('Agent Name', '')
            agent_number = rows[i].get('Agent Number', '')
            terminal_id = rows[i].get('Terminal ID', '')
            print(f"  {i+1}. {agent_name} (Number: {agent_number}, Terminal: {terminal_id})")
            
        # Verify uniqueness
        agent_names = [row.get('Agent Name', '').strip().upper() for row in rows]
        unique_names = set(agent_names)
        
        print(f"\nUniqueness verification:")
        print(f"  Total agent names: {len(agent_names)}")
        print(f"  Unique agent names: {len(unique_names)}")
        print(f"  Duplicates: {len(agent_names) - len(unique_names)}")
        
        if len(agent_names) == len(unique_names):
            print("  ✓ All agents are unique!")
        else:
            print("  ✗ Still contains duplicates")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_unique_file()