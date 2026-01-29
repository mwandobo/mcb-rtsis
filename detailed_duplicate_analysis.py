#!/usr/bin/env python3
"""
Detailed analysis of duplicate records in agent-from-cbs.csv
"""

import csv

def analyze_duplicate_details():
    """Show detailed information for duplicate records"""
    
    # Read all data
    with open('agent-from-cbs.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print("Available columns:", list(rows[0].keys()))
    print()
    
    # Known duplicate agent names and their row numbers (1-based)
    duplicates = {
        'GRACE STEPHEN GEORGE': [447, 494],
        'KICHONGE MAHENDE MASERO': [511, 526], 
        'REDEMPTA ROBERT MAX': [62, 211]
    }
    
    for agent_name, row_numbers in duplicates.items():
        print(f"=== DUPLICATE ANALYSIS: {agent_name} ===")
        
        for row_num in row_numbers:
            row_index = row_num - 2  # Convert to 0-based index (subtract 2 for header)
            if row_index < len(rows):
                row = rows[row_index]
                print(f"\nRow {row_num}:")
                
                # Show all non-empty fields
                for key, value in row.items():
                    if value and value.strip():
                        print(f"  {key}: {value}")
        
        print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    analyze_duplicate_details()