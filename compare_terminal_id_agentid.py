#!/usr/bin/env python3
"""
Script to compare Terminal ID from mwalimu_unique_agents.csv with AGENTID from agent-from-cbs.csv
"""

import csv

def read_mwalimu_agents(filename):
    """Read agents from mwalimu_unique_agents.csv using Terminal ID"""
    agents = {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                terminal_id = row.get('Terminal ID', '').strip()
                agent_name = row.get('Agent Name', '').strip()
                agent_number = row.get('Agent Number', '').strip()
                
                if terminal_id:
                    agents[terminal_id] = {
                        'Terminal_ID': terminal_id,
                        'Agent_Number': agent_number,
                        'Agent_Name': agent_name,
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
                    }
                        
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return {}
    
    return agents

def read_cbs_agents(filename):
    """Read agents from agent-from-cbs.csv using AGENTID"""
    agents = {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                agent_id = row.get('AGENTID', '').strip()
                agent_name = row.get('AGENTNAME', '').strip()
                
                if agent_id:
                    agents[agent_id] = {
                        'AGENTID': agent_id,
                        'AGENTNAME': agent_name,
                        'TILLNUMBER': row.get('TILLNUMBER', ''),
                        'BUSINESSFORM': row.get('BUSINESSFORM', ''),
                        'AGENTPRINCIPAL': row.get('AGENTPRINCIPAL', ''),
                        'AGENTPRINCIPALNAME': row.get('AGENTPRINCIPALNAME', ''),
                        'GENDER': row.get('GENDER', ''),
                        'REGISTRATIONDATE': row.get('REGISTRATIONDATE', ''),
                        'AGENTSTATUS': row.get('AGENTSTATUS', ''),
                        'AGENTTYPE': row.get('AGENTTYPE', ''),
                        'REGION': row.get('REGION', ''),
                        'DISTRICT': row.get('DISTRICT', ''),
                        'WARD': row.get('WARD', ''),
                        'STREET': row.get('STREET', ''),
                        'COUNTRY': row.get('COUNTRY', ''),
                        'GPSCOORDINATES': row.get('GPSCOORDINATES', '')
                    }
                        
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return {}
    
    return agents

def main():
    print("=== TERMINAL ID vs AGENTID COMPARISON ===")
    print("Comparing Terminal ID from MWALIMU with AGENTID from CBS")
    print()
    
    # Read agents from both files
    print("Reading mwalimu_unique_agents.csv...")
    mwalimu_agents = read_mwalimu_agents('mwalimu_unique_agents.csv')
    print(f"Found {len(mwalimu_agents)} agents with Terminal IDs in mwalimu_unique_agents.csv")
    
    print("Reading agent-from-cbs.csv...")
    cbs_agents = read_cbs_agents('agent-from-cbs.csv')
    print(f"Found {len(cbs_agents)} agents with AGENTIDs in agent-from-cbs.csv")
    
    # Show sample formats
    print(f"\nSample Terminal ID formats from MWALIMU:")
    sample_terminal_ids = list(mwalimu_agents.keys())[:5]
    for tid in sample_terminal_ids:
        print(f"  {tid}")
    
    print(f"\nSample AGENTID formats from CBS:")
    sample_agent_ids = list(cbs_agents.keys())[:5]
    for aid in sample_agent_ids:
        print(f"  {aid}")
    
    # Find matches and missing agents
    matched_agents = set()
    missing_agents = []
    match_details = []
    
    print(f"\nComparing Terminal IDs with AGENTIDs...")
    
    for terminal_id, mwalimu_data in mwalimu_agents.items():
        if terminal_id in cbs_agents:
            matched_agents.add(terminal_id)
            match_details.append({
                'Terminal_ID_AGENTID': terminal_id,
                'Mwalimu_Name': mwalimu_data['Agent_Name'],
                'CBS_Name': cbs_agents[terminal_id]['AGENTNAME'],
                'Mwalimu_Agent_Number': mwalimu_data['Agent_Number'],
                'Names_Match': mwalimu_data['Agent_Name'].upper().strip() == cbs_agents[terminal_id]['AGENTNAME'].upper().strip()
            })
        else:
            missing_agents.append(mwalimu_data)
    
    print(f"\nRESULTS:")
    print(f"- Total agents in mwalimu_unique_agents: {len(mwalimu_agents)}")
    print(f"- Matched by Terminal ID = AGENTID: {len(matched_agents)}")
    print(f"- Agents NOT in CBS: {len(missing_agents)}")
    
    # Save match details
    if match_details:
        print(f"\nSaving match details to 'terminal_id_agentid_matches.csv'...")
        with open('terminal_id_agentid_matches.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Terminal_ID_AGENTID', 'Mwalimu_Name', 'CBS_Name', 'Mwalimu_Agent_Number', 'Names_Match']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for match in match_details:
                writer.writerow(match)
        print(f"Saved {len(match_details)} matches")
        
        # Show name mismatches
        name_mismatches = [m for m in match_details if not m['Names_Match']]
        if name_mismatches:
            print(f"\nFound {len(name_mismatches)} cases where Terminal ID = AGENTID but names differ:")
            for i, mismatch in enumerate(name_mismatches[:10]):  # Show first 10
                print(f"  ID {mismatch['Terminal_ID_AGENTID']}:")
                print(f"    Mwalimu: {mismatch['Mwalimu_Name']}")
                print(f"    CBS:     {mismatch['CBS_Name']}")
                print()
        
        # Show some successful matches
        successful_matches = [m for m in match_details if m['Names_Match']]
        if successful_matches:
            print(f"Sample successful matches (Terminal ID = AGENTID and names match):")
            for i, match in enumerate(successful_matches[:5]):
                print(f"  ID {match['Terminal_ID_AGENTID']}: {match['Mwalimu_Name']}")
    
    # Save missing agents
    if missing_agents:
        output_filename = 'agents_missing_from_cbs_by_terminal_id.csv'
        print(f"\nWriting {len(missing_agents)} missing agents to {output_filename}...")
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if missing_agents:
                fieldnames = missing_agents[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in missing_agents:
                    writer.writerow(agent)
                
                print(f"Successfully wrote {len(missing_agents)} missing agents to {output_filename}")
    else:
        print("All agents from mwalimu_unique_agents.csv were found in agent-from-cbs.csv by Terminal ID!")
    
    # Show some sample missing agents
    if missing_agents:
        print(f"\nSample missing agents (first 5):")
        for i, agent in enumerate(missing_agents[:5]):
            print(f"  {i+1}. Terminal ID: {agent['Terminal_ID']}")
            print(f"     Agent Number: {agent['Agent_Number']}")
            print(f"     Name: {agent['Agent_Name']}")
            print(f"     Region: {agent['Region']}")
            print()

if __name__ == "__main__":
    main()