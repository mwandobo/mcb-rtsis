#!/usr/bin/env python3
"""
Hybrid comparison script:
1. First compare by last 8 characters of Terminal ID vs AGENTID
2. For unmatched agents, fallback to fuzzy name matching
"""

import csv
import re
from difflib import SequenceMatcher

def normalize_name(name):
    """Normalize agent name for better matching"""
    if not name:
        return ""
    
    # Convert to uppercase
    name = name.upper().strip()
    
    # Remove common suffixes and prefixes
    patterns_to_remove = [
        r'\s+T/A\s+.*',  # Remove "T/A ..." part
        r'\s+C/O\s+.*',  # Remove "C/O ..." part
        r'\s+LTD\.?',    # Remove "LTD" at end
        r'\s+LIMITED',   # Remove "LIMITED" at end
        r'\s+CO\.?\s+LTD\.?',  # Remove "CO LTD" at end
        r'\s+COMPANY',   # Remove "COMPANY" at end
        r'\s+\d+',       # Remove trailing numbers like "2", "3"
        r'\s+-\s*\d+',   # Remove trailing " - 2", " -3"
        r'\s+\(\w+\s+\w+:.*\)',  # Remove "(Agent Number: ...)"
    ]
    
    for pattern in patterns_to_remove:
        name = re.sub(pattern, '', name)
    
    # Remove extra whitespace and punctuation
    name = re.sub(r'[^\w\s]', ' ', name)  # Replace punctuation with space
    name = re.sub(r'\s+', ' ', name)      # Normalize whitespace
    name = name.strip()
    
    return name

def similarity_score(name1, name2):
    """Calculate similarity score between two names"""
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)
    
    if not norm1 or not norm2:
        return 0.0
    
    # Exact match after normalization
    if norm1 == norm2:
        return 1.0
    
    # Check if one is contained in the other (for cases like "SKYSCAPE" vs "SKYSCAPE INTERNATIONAL")
    if norm1 in norm2 or norm2 in norm1:
        shorter = min(norm1, norm2, key=len)
        longer = max(norm1, norm2, key=len)
        if len(shorter) >= 10:  # Only for reasonably long names
            return 0.9
    
    # Use sequence matcher for fuzzy matching
    return SequenceMatcher(None, norm1, norm2).ratio()

def find_best_name_match(target_name, candidate_agents, threshold=0.85):
    """Find the best name match for a target name from a dict of candidates"""
    best_match = None
    best_score = 0.0
    best_agent_data = None
    
    for agent_id, agent_data in candidate_agents.items():
        candidate_name = agent_data['AGENTNAME']
        score = similarity_score(target_name, candidate_name)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate_name
            best_agent_data = agent_data
    
    return best_match, best_score, best_agent_data

def read_mwalimu_agents(filename):
    """Read agents from mwalimu_unique_agents.csv"""
    agents = {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                terminal_id = row.get('Terminal ID', '').strip()
                agent_name = row.get('Agent Name', '').strip()
                agent_number = row.get('Agent Number', '').strip()
                
                if terminal_id:
                    last_8_chars = terminal_id[-8:] if len(terminal_id) >= 8 else terminal_id
                    agents[terminal_id] = {
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
    """Read agents from agent-from-cbs.csv"""
    agents_by_last8 = {}  # For last 8 chars matching
    agents_by_id = {}     # For name matching fallback
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                agent_id = row.get('AGENTID', '').strip()
                agent_name = row.get('AGENTNAME', '').strip()
                
                if agent_id:
                    last_8_chars = agent_id[-8:] if len(agent_id) >= 8 else agent_id
                    
                    agent_data = {
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
                    
                    agents_by_last8[last_8_chars] = agent_data
                    agents_by_id[agent_id] = agent_data
                        
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return {}, {}
    
    return agents_by_last8, agents_by_id

def main():
    print("=== HYBRID COMPARISON: LAST 8 CHARS + NAME FALLBACK ===")
    print("Step 1: Match by last 8 characters of Terminal ID vs AGENTID")
    print("Step 2: For unmatched agents, try fuzzy name matching")
    print()
    
    # Read agents from both files
    print("Reading mwalimu_unique_agents.csv...")
    mwalimu_agents = read_mwalimu_agents('mwalimu_unique_agents.csv')
    print(f"Found {len(mwalimu_agents)} agents in mwalimu_unique_agents.csv")
    
    print("Reading agent-from-cbs.csv...")
    cbs_agents_by_last8, cbs_agents_by_id = read_cbs_agents('agent-from-cbs.csv')
    print(f"Found {len(cbs_agents_by_last8)} agents in agent-from-cbs.csv")
    
    # Step 1: Match by last 8 characters
    print(f"\n=== STEP 1: MATCHING BY LAST 8 CHARACTERS ===")
    last8_matches = []
    unmatched_agents = {}
    
    for terminal_id, mwalimu_data in mwalimu_agents.items():
        last_8_chars = mwalimu_data['Last_8_Chars']
        
        if last_8_chars in cbs_agents_by_last8:
            cbs_data = cbs_agents_by_last8[last_8_chars]
            last8_matches.append({
                'Match_Type': 'last_8_chars',
                'Terminal_ID': terminal_id,
                'AGENTID': cbs_data['AGENTID'],
                'Last_8_Chars': last_8_chars,
                'Mwalimu_Name': mwalimu_data['Agent_Name'],
                'CBS_Name': cbs_data['AGENTNAME'],
                'Mwalimu_Agent_Number': mwalimu_data['Agent_Number'],
                'Names_Match': mwalimu_data['Agent_Name'].upper().strip() == cbs_data['AGENTNAME'].upper().strip(),
                'Similarity_Score': 1.0
            })
        else:
            unmatched_agents[terminal_id] = mwalimu_data
    
    print(f"Matched by last 8 characters: {len(last8_matches)}")
    print(f"Unmatched agents for name fallback: {len(unmatched_agents)}")
    
    # Step 2: Name matching fallback for unmatched agents
    print(f"\n=== STEP 2: NAME MATCHING FALLBACK ===")
    name_matches = []
    still_unmatched = []
    
    # Remove already matched CBS agents from consideration
    matched_cbs_ids = {match['AGENTID'] for match in last8_matches}
    available_cbs_agents = {aid: data for aid, data in cbs_agents_by_id.items() if aid not in matched_cbs_ids}
    
    print(f"Available CBS agents for name matching: {len(available_cbs_agents)}")
    
    for terminal_id, mwalimu_data in unmatched_agents.items():
        target_name = mwalimu_data['Agent_Name']
        best_name, score, best_agent_data = find_best_name_match(target_name, available_cbs_agents)
        
        if best_name and best_agent_data:
            name_matches.append({
                'Match_Type': 'fuzzy_name',
                'Terminal_ID': terminal_id,
                'AGENTID': best_agent_data['AGENTID'],
                'Last_8_Chars': mwalimu_data['Last_8_Chars'],
                'Mwalimu_Name': target_name,
                'CBS_Name': best_name,
                'Mwalimu_Agent_Number': mwalimu_data['Agent_Number'],
                'Names_Match': target_name.upper().strip() == best_name.upper().strip(),
                'Similarity_Score': score
            })
            # Remove this CBS agent from further consideration
            available_cbs_agents.pop(best_agent_data['AGENTID'], None)
        else:
            still_unmatched.append(mwalimu_data)
    
    print(f"Additional matches found by name: {len(name_matches)}")
    print(f"Still unmatched: {len(still_unmatched)}")
    
    # Combine all matches
    all_matches = last8_matches + name_matches
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"- Total agents in mwalimu_unique_agents: {len(mwalimu_agents)}")
    print(f"- Matched by last 8 characters: {len(last8_matches)}")
    print(f"- Additional matches by name: {len(name_matches)}")
    print(f"- Total matches: {len(all_matches)}")
    print(f"- Still missing from CBS: {len(still_unmatched)}")
    print(f"- Match rate: {len(all_matches)/len(mwalimu_agents)*100:.1f}%")
    
    # Save all matches
    if all_matches:
        print(f"\nSaving all matches to 'hybrid_matches.csv'...")
        with open('hybrid_matches.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Match_Type', 'Terminal_ID', 'AGENTID', 'Last_8_Chars', 'Mwalimu_Name', 'CBS_Name', 'Mwalimu_Agent_Number', 'Names_Match', 'Similarity_Score']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for match in all_matches:
                writer.writerow(match)
        print(f"Saved {len(all_matches)} total matches")
        
        # Show sample name matches
        if name_matches:
            print(f"\nSample fuzzy name matches found:")
            for i, match in enumerate(name_matches[:10]):
                print(f"  {i+1}. Score: {match['Similarity_Score']:.3f}")
                print(f"     MWALIMU: {match['Mwalimu_Name']}")
                print(f"     CBS:     {match['CBS_Name']}")
                print(f"     Terminal ID: {match['Terminal_ID']} | AGENTID: {match['AGENTID']}")
                print()
    
    # Save still unmatched agents
    if still_unmatched:
        output_filename = 'agents_still_missing_after_hybrid.csv'
        print(f"Writing {len(still_unmatched)} still missing agents to {output_filename}...")
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if still_unmatched:
                fieldnames = still_unmatched[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in still_unmatched:
                    writer.writerow(agent)
                
                print(f"Successfully wrote {len(still_unmatched)} still missing agents")
        
        # Show sample still missing
        print(f"\nSample agents still missing after hybrid matching:")
        for i, agent in enumerate(still_unmatched[:5]):
            print(f"  {i+1}. Terminal ID: {agent['Terminal_ID']} (Last 8: {agent['Last_8_Chars']})")
            print(f"     Name: {agent['Agent_Name']}")
            print(f"     Region: {agent['Region']}, District: {agent['District']}")
            print()
    else:
        print("All agents from mwalimu_unique_agents.csv were matched using hybrid approach!")

if __name__ == "__main__":
    main()