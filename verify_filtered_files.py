#!/usr/bin/env python3
"""
Verify the filtered CSV files
"""

import csv

def verify_filtered_files():
    """Verify both filtered files"""
    
    files_to_check = [
        ('agents_with_complete_location.csv', 'Complete Location Info'),
        ('agents_missing_tin_and_license.csv', 'Missing TIN and Business License')
    ]
    
    for filename, description in files_to_check:
        print(f"=== {description.upper()} ===")
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
            print(f"File: {filename}")
            print(f"Total records: {len(rows)}")
            
            if len(rows) > 0:
                print(f"Columns: {len(rows[0].keys())}")
                print(f"\nFirst 3 records:")
                
                for i in range(min(3, len(rows))):
                    agent = rows[i]
                    agent_name = agent.get('Agent Name', '')
                    region = agent.get('Region', '')
                    district = agent.get('District', '')
                    tin = agent.get('Tin', '')
                    license_info = agent.get('Business Licence No. Issuer And Date', '')
                    
                    print(f"  {i+1}. {agent_name}")
                    print(f"     Region: '{region}', District: '{district}'")
                    print(f"     TIN: '{tin}', License: '{license_info}'")
            
        except Exception as e:
            print(f"Error reading {filename}: {e}")
        
        print()

if __name__ == "__main__":
    verify_filtered_files()