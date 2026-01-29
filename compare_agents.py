#!/usr/bin/env python3
"""
Script to find agents that are in MWALIMU-WAKALA.csv but not in agent-from-cbs.csv
"""

import csv
import sys

def read_csv_agents(filename, agent_name_col, agent_number_col=None):
    """Read agents from CSV file and return a set of agent names"""
    agents = set()
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # Get agent name only
                agent_name = row.get(agent_name_col, '').strip()
                
                # Use only agent name as identifier
                if agent_name:
                    agents.add(agent_name.upper())
                    
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return set()
    
    return agents

def get_agent_details_from_mwalimu(filename, missing_agents):
    """Get full details of missing agents from MWALIMU-WAKALA.csv"""
    agent_details = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                agent_name = row.get('Agent Name', '').strip()
                
                # Use only agent name as identifier
                if agent_name and agent_name.upper() in missing_agents:
                    agent_details.append({
                        'Agent_Name': agent_name,
                        'Agent_Number': row.get('Agent Number', ''),
                        'Terminal_ID': row.get('Terminal ID', ''),
                        'Current_Business': row.get('Current Business', ''),
                        'Business_Form': row.get('Business Form', ''),
                        'Region': row.get('Region', ''),
                        'District': row.get('District', ''),
                        'Physical_Location': row.get('Physical Location And Postal Address', ''),
                        'GPS_Coordinates': row.get('Gps Coordinates', ''),
                        'Contact': row.get('contact', ''),
                        'TIN': row.get('Tin', ''),
                        'Business_Licence': row.get('Business Licence No. Issuer And Date', ''),
                        'Shareholders_Name': row.get("Shareholder's Name", ''),
                        'Shareholders_ID': row.get("Shareholders Identity Card No.", ''),
                        'Agency_Operators': row.get('Names Of Agency Operators', ''),
                        'Operator_Phone': row.get('Phone Number of Operator', ''),
                        'Education_Qualification': row.get('Education Qualification Of Operators', ''),
                        'Business_Experience': row.get('Business Experience In Years Supported By Relevant Documents', '')
                    })
                        
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    return agent_details

def main():
    # Read agents from MWALIMU-WAKALA.csv
    print("Reading MWALIMU-WAKALA.csv...")
    mwalimu_agents = read_csv_agents('MWALIMU-WAKALA.csv', 'Agent Name', 'Agent Number')
    print(f"Found {len(mwalimu_agents)} agents in MWALIMU-WAKALA.csv")
    
    # Read agents from agent-from-cbs.csv
    print("Reading agent-from-cbs.csv...")
    cbs_agents = read_csv_agents('agent-from-cbs.csv', 'AGENTNAME', 'AGENTID')
    print(f"Found {len(cbs_agents)} agents in agent-from-cbs.csv")
    
    # Find agents in MWALIMU-WAKALA but not in CBS
    agents_not_in_cbs = mwalimu_agents - cbs_agents
    
    print(f"Total agents in MWALIMU-WAKALA but NOT in CBS: {len(agents_not_in_cbs)}")
    
    if agents_not_in_cbs:
        # Get full details of missing agents
        print("Getting full details of missing agents...")
        missing_agent_details = get_agent_details_from_mwalimu('MWALIMU-WAKALA.csv', agents_not_in_cbs)
        
        # Write to CSV file
        output_filename = 'agents_missing_from_cbs.csv'
        print(f"Writing results to {output_filename}...")
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if missing_agent_details:
                fieldnames = missing_agent_details[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in missing_agent_details:
                    writer.writerow(agent)
                
                print(f"Successfully wrote {len(missing_agent_details)} agents to {output_filename}")
            else:
                print("No agent details found to write")
    else:
        print("No agents found in MWALIMU-WAKALA.csv that are missing from agent-from-cbs.csv")

if __name__ == "__main__":
    main()