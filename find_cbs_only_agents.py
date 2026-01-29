#!/usr/bin/env python3
"""
Find agents that are in agent-from-cbs.csv but NOT in mwalimu_unique_agents.csv
Output with same headers as mwalimu_unique_agents.csv
"""

import csv

def find_cbs_only_agents():
    """Find agents in CBS but not in MWALIMU and create CSV with MWALIMU headers"""
    
    # Read MWALIMU unique agents
    mwalimu_agents = set()
    mwalimu_headers = []
    
    try:
        with open('mwalimu_unique_agents.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            mwalimu_headers = reader.fieldnames
            
            for row in reader:
                agent_name = row.get('Agent Name', '').strip()
                if agent_name:
                    mwalimu_agents.add(agent_name.upper())
                    
        print(f"Found {len(mwalimu_agents)} unique agents in MWALIMU")
        print(f"MWALIMU headers: {mwalimu_headers}")
        
    except Exception as e:
        print(f"Error reading mwalimu_unique_agents.csv: {e}")
        return
    
    # Read CBS agents
    cbs_agents = []
    cbs_agent_names = set()
    
    try:
        with open('agent-from-cbs.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                agent_name = row.get('AGENTNAME', '').strip()
                if agent_name:
                    cbs_agents.append(row)
                    cbs_agent_names.add(agent_name.upper())
                    
        print(f"Found {len(cbs_agent_names)} unique agent names in CBS")
        print(f"Total CBS records: {len(cbs_agents)}")
        
    except Exception as e:
        print(f"Error reading agent-from-cbs.csv: {e}")
        return
    
    # Find agents in CBS but not in MWALIMU
    cbs_only_agents = []
    
    for cbs_agent in cbs_agents:
        agent_name = cbs_agent.get('AGENTNAME', '').strip()
        if agent_name and agent_name.upper() not in mwalimu_agents:
            # Map CBS fields to MWALIMU headers
            mapped_agent = {}
            
            # Initialize all MWALIMU fields as empty
            for header in mwalimu_headers:
                mapped_agent[header] = ''
            
            # Map available CBS fields to MWALIMU fields
            field_mapping = {
                'Agent Name': cbs_agent.get('AGENTNAME', ''),
                'Agent Number': cbs_agent.get('AGENTID', ''),
                'Terminal ID': cbs_agent.get('TILLNUMBER', ''),
                'Business Form': cbs_agent.get('BUSINESSFORM', ''),
                'Region': cbs_agent.get('REGION', ''),
                'District': cbs_agent.get('DISTRICT', ''),
                'Physical Location And Postal Address': f"{cbs_agent.get('WARD', '')} {cbs_agent.get('STREET', '')} {cbs_agent.get('HOUSENUMBER', '')}".strip(),
                'Gps Coordinates': cbs_agent.get('GPSCOORDINATES', ''),
                'contact': '',  # Not available in CBS
                'Tin': '',  # Not available in CBS
                'Certificate Of Incorporation': cbs_agent.get('CERTINCORPORATION', ''),
                'Business Licence No. Issuer And Date': '',  # Not available in CBS
                "Shareholder's Name": '',  # Not available in CBS
                'Shareholders Identity Card No.': '',  # Not available in CBS
                'Names Of Agency Operators': '',  # Not available in CBS
                'Phone Number of Operator': '',  # Not available in CBS
                'Education Qualification Of Operators': '',  # Not available in CBS
                'Business Experience In Years Supported By Relevant Documents': '',  # Not available in CBS
                'Current Business': cbs_agent.get('AGENTTYPE', '')  # Using agent type as business type
            }
            
            # Apply the mapping
            for mwalimu_field, cbs_value in field_mapping.items():
                if mwalimu_field in mapped_agent:
                    mapped_agent[mwalimu_field] = cbs_value.strip() if cbs_value else ''
            
            cbs_only_agents.append(mapped_agent)
    
    print(f"Found {len(cbs_only_agents)} agents in CBS but NOT in MWALIMU")
    
    # Write to CSV file with MWALIMU headers
    output_filename = 'agents_in_cbs_not_in_mwalimu.csv'
    
    try:
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if cbs_only_agents and mwalimu_headers:
                writer = csv.DictWriter(csvfile, fieldnames=mwalimu_headers)
                
                writer.writeheader()
                for agent in cbs_only_agents:
                    writer.writerow(agent)
                
                print(f"✓ Successfully wrote {len(cbs_only_agents)} agents to {output_filename}")
                
                # Show sample records
                print(f"\nSample records:")
                for i, agent in enumerate(cbs_only_agents[:3]):
                    print(f"  {i+1}. {agent.get('Agent Name', '')} (ID: {agent.get('Agent Number', '')}, Region: {agent.get('Region', '')})")
                    
            else:
                print("No agents found in CBS that are not in MWALIMU")
                
    except Exception as e:
        print(f"Error writing {output_filename}: {e}")
    
    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"MWALIMU unique agents: {len(mwalimu_agents)}")
    print(f"CBS unique agent names: {len(cbs_agent_names)}")
    print(f"Agents in CBS but NOT in MWALIMU: {len(cbs_only_agents)}")
    print(f"Overlap (agents in both): {len(mwalimu_agents & cbs_agent_names)}")

if __name__ == "__main__":
    find_cbs_only_agents()