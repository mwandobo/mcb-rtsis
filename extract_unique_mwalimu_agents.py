#!/usr/bin/env python3
"""
Script to extract unique agents from MWALIMU-WAKALA.csv
Keeps only the first occurrence of each agent name
"""

import csv

def extract_unique_agents():
    """Extract unique agents and save to CSV"""
    
    unique_agents = []
    seen_names = set()
    
    try:
        with open('MWALIMU-WAKALA.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, start=2):
                agent_name = row.get('Agent Name', '').strip()
                
                if agent_name:
                    agent_name_upper = agent_name.upper()
                    
                    # Keep only first occurrence of each agent name
                    if agent_name_upper not in seen_names:
                        seen_names.add(agent_name_upper)
                        unique_agents.append(row)
                        
    except Exception as e:
        print(f"Error reading MWALIMU-WAKALA.csv: {e}")
        return
    
    print(f"Found {len(unique_agents)} unique agents")
    
    # Write unique agents to new CSV file
    output_filename = 'mwalimu_unique_agents.csv'
    
    try:
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if unique_agents:
                fieldnames = unique_agents[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in unique_agents:
                    writer.writerow(agent)
                
                print(f"Successfully wrote {len(unique_agents)} unique agents to {output_filename}")
            else:
                print("No unique agents found to write")
                
    except Exception as e:
        print(f"Error writing to {output_filename}: {e}")

if __name__ == "__main__":
    extract_unique_agents()