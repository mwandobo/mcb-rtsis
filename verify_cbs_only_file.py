#!/usr/bin/env python3
"""
Verify the CBS-only agents file
"""

import csv

def verify_cbs_only_file():
    """Verify the CBS-only agents file"""
    
    try:
        with open('agents_in_cbs_not_in_mwalimu.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        print(f"=== AGENTS IN CBS BUT NOT IN MWALIMU ===")
        print(f"Total records: {len(rows)}")
        print(f"Headers match MWALIMU format: {len(reader.fieldnames)} columns")
        print(f"Headers: {reader.fieldnames}")
        
        if len(rows) > 0:
            print(f"\nFirst 5 records with available data:")
            
            for i in range(min(5, len(rows))):
                agent = rows[i]
                agent_name = agent.get('Agent Name', '')
                agent_id = agent.get('Agent Number', '')
                region = agent.get('Region', '')
                district = agent.get('District', '')
                business_form = agent.get('Business Form', '')
                current_business = agent.get('Current Business', '')
                
                print(f"\n  {i+1}. {agent_name}")
                print(f"     Agent ID: {agent_id}")
                print(f"     Region: '{region}', District: '{district}'")
                print(f"     Business Form: '{business_form}'")
                print(f"     Current Business: '{current_business}'")
        
        # Check data completeness
        print(f"\n=== DATA COMPLETENESS ANALYSIS ===")
        
        fields_to_check = ['Agent Name', 'Agent Number', 'Region', 'District', 'Business Form', 'Current Business']
        
        for field in fields_to_check:
            non_empty = sum(1 for row in rows if row.get(field, '').strip())
            percentage = (non_empty / len(rows) * 100) if len(rows) > 0 else 0
            print(f"{field}: {non_empty}/{len(rows)} ({percentage:.1f}%) have data")
            
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    verify_cbs_only_file()