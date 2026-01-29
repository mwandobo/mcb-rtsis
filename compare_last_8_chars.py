#!/usr/bin/env python3
"""
Script to compare last 8 characters of Terminal ID from mwalimu_unique_agents.csv 
with last 8 characters of AGENTID from agent-from-cbs.csv
"""

import csv

def read_mwalimu_agents(filename):
    """Read agents from mwalimu_unique_agents.csv using last 8 chars of Terminal ID"""
    agents = {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                terminal_id = row.get('Terminal ID', '').strip()
                agent_name = row.get('Agent Name', '').strip()
                agent_number = row.get('Agent Number', '').strip()
                
                if terminal_id and len(terminal_id) >= 8:
                    last_8_chars = terminal_id[-8:]  # Get last 8 characters
                    agents[last_8_chars] = {
                        'Terminal_ID': terminal_id,
                        'Last_8_Chars': last_8_chars,
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
    """Read agents from agent-from-cbs.csv using last 8 chars of AGENTID"""
    agents = {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                agent_id = row.get('AGENTID', '').strip()
                agent_name = row.get('AGENTNAME', '').strip()
                
                if agent_id and len(agent_id) >= 8:
                    last_8_chars = agent_id[-8:]  # Get last 8 characters
                    agents[last_8_chars] = {
                        'AGENTID': agent_id,
                        'Last_8_Chars': last_8_chars,
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
    print("=== LAST 8 CHARACTERS COMPARISON ===")
    print("Comparing last 8 chars of Terminal ID from MWALIMU with last 8 chars of AGENTID from CBS")
    print()
    
    # Read agents from both files
    print("Reading mwalimu_unique_agents.csv...")
    mwalimu_agents = read_mwalimu_agents('mwalimu_unique_agents.csv')
    print(f"Found {len(mwalimu_agents)} agents with Terminal IDs (≥8 chars) in mwalimu_unique_agents.csv")
    
    print("Reading agent-from-cbs.csv...")
    cbs_agents = read_cbs_agents('agent-from-cbs.csv')
    print(f"Found {len(cbs_agents)} agents with AGENTIDs (≥8 chars) in agent-from-cbs.csv")
    
    # Show sample formats
    print(f"\nSample Terminal ID → Last 8 chars from MWALIMU:")
    sample_mwalimu = list(mwalimu_agents.items())[:5]
    for last_8, data in sample_mwalimu:
        print(f"  {data['Terminal_ID']} → {last_8}")
    
    print(f"\nSample AGENTID → Last 8 chars from CBS:")
    sample_cbs = list(cbs_agents.items())[:5]
    for last_8, data in sample_cbs:
        print(f"  {data['AGENTID']} → {last_8}")
    
    # Find matches and missing agents
    matched_agents = set()
    missing_agents = []
    match_details = []
    
    print(f"\nComparing last 8 characters...")
    
    for last_8_chars, mwalimu_data in mwalimu_agents.items():
        if last_8_chars in cbs_agents:
            matched_agents.add(last_8_chars)
            match_details.append({
                'Last_8_Chars': last_8_chars,
                'Mwalimu_Terminal_ID': mwalimu_data['Terminal_ID'],
                'CBS_AGENTID': cbs_agents[last_8_chars]['AGENTID'],
                'Mwalimu_Name': mwalimu_data['Agent_Name'],
                'CBS_Name': cbs_agents[last_8_chars]['AGENTNAME'],
                'Mwalimu_Agent_Number': mwalimu_data['Agent_Number'],
                'Names_Match': mwalimu_data['Agent_Name'].upper().strip() == cbs_agents[last_8_chars]['AGENTNAME'].upper().strip()
            })
        else:
            missing_agents.append(mwalimu_data)
    
    print(f"\nRESULTS:")
    print(f"- Total agents in mwalimu_unique_agents: {len(mwalimu_agents)}")
    print(f"- Matched by last 8 characters: {len(matched_agents)}")
    print(f"- Agents NOT in CBS: {len(missing_agents)}")
    
    # Save match details
    if match_details:
        print(f"\nSaving match details to 'last_8_chars_matches.csv'...")
        with open('last_8_chars_matches.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Last_8_Chars', 'Mwalimu_Terminal_ID', 'CBS_AGENTID', 'Mwalimu_Name', 'CBS_Name', 'Mwalimu_Agent_Number', 'Names_Match']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for match in match_details:
                writer.writerow(match)
        print(f"Saved {len(match_details)} matches")
        
        # Show name mismatches
        name_mismatches = [m for m in match_details if not m['Names_Match']]
        if name_mismatches:
            print(f"\nFound {len(name_mismatches)} cases where last 8 chars match but names differ:")
            for i, mismatch in enumerate(name_mismatches[:10]):  # Show first 10
                print(f"  Last 8: {mismatch['Last_8_Chars']}")
                print(f"    Terminal ID: {mismatch['Mwalimu_Terminal_ID']} | AGENTID: {mismatch['CBS_AGENTID']}")
                print(f"    Mwalimu: {mismatch['Mwalimu_Name']}")
                print(f"    CBS:     {mismatch['CBS_Name']}")
                print()
        
        # Show some successful matches
        successful_matches = [m for m in match_details if m['Names_Match']]
        if successful_matches:
            print(f"Sample successful matches (last 8 chars match and names match):")
            for i, match in enumerate(successful_matches[:5]):
                print(f"  Last 8: {match['Last_8_Chars']}")
                print(f"    Terminal ID: {match['Mwalimu_Terminal_ID']} | AGENTID: {match['CBS_AGENTID']}")
                print(f"    Name: {match['Mwalimu_Name']}")
                print()
    
    # Save missing agents
    if missing_agents:
        output_filename = 'agents_missing_from_cbs_by_last_8_chars.csv'
        print(f"Writing {len(missing_agents)} missing agents to {output_filename}...")
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if missing_agents:
                fieldnames = missing_agents[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in missing_agents:
                    writer.writerow(agent)
                
                print(f"Successfully wrote {len(missing_agents)} missing agents to {output_filename}")
    else:
        print("All agents from mwalimu_unique_agents.csv were found in agent-from-cbs.csv by last 8 characters!")
    
    # Show some sample missing agents
    if missing_agents:
        print(f"\nSample missing agents (first 5):")
        for i, agent in enumerate(missing_agents[:5]):
            print(f"  {i+1}. Terminal ID: {agent['Terminal_ID']} (Last 8: {agent['Last_8_Chars']})")
            print(f"     Agent Number: {agent['Agent_Number']}")
            print(f"     Name: {agent['Agent_Name']}")
            print(f"     Region: {agent['Region']}")
            print()

if __name__ == "__main__":
    main()