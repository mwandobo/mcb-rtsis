#!/usr/bin/env python3
"""
Script to find agents that are in MWALIMU-WAKALA.csv but not in agent-from-cbs.csv
Uses flexible matching to handle name variations
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
        r'\s+T/A\s+.*$',  # Remove "T/A ..." part
        r'\s+C/O\s+.*$',  # Remove "C/O ..." part
        r'\s+LTD\.?$',    # Remove "LTD" at end
        r'\s+LIMITED$',   # Remove "LIMITED" at end
        r'\s+CO\.?\s+LTD\.?$',  # Remove "CO LTD" at end
        r'\s+COMPANY$',   # Remove "COMPANY" at end
        r'\s+\d+$',       # Remove trailing numbers like "2", "3"
        r'\s+-\s*\d+$',   # Remove trailing " - 2", " -3"
        r'\s+\(\w+\s+\w+:.*\)$',  # Remove "(Agent Number: ...)"
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

def find_best_match(target_name, candidate_names, threshold=0.85):
    """Find the best match for a target name from a list of candidates"""
    best_match = None
    best_score = 0.0
    
    for candidate in candidate_names:
        score = similarity_score(target_name, candidate)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate
    
    return best_match, best_score

def read_csv_agents_flexible(filename, agent_name_col, agent_number_col=None):
    """Read agents from CSV file and return both normalized and original names"""
    agents = {}  # normalized_name -> original_name
    agent_numbers = {}  # normalized_name -> agent_number
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                agent_name = row.get(agent_name_col, '').strip()
                agent_number = ''
                
                if agent_number_col and agent_number_col in row:
                    agent_number = row.get(agent_number_col, '').strip()
                
                if agent_name:
                    normalized = normalize_name(agent_name)
                    if normalized:
                        agents[normalized] = agent_name
                        if agent_number and agent_number != '0' and agent_number != '#N/A':
                            agent_numbers[normalized] = agent_number
                        
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return {}, {}
    
    return agents, agent_numbers

def get_agent_details_from_mwalimu(filename, missing_agents):
    """Get full details of missing agents from MWALIMU-WAKALA.csv"""
    agent_details = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                agent_name = row.get('Agent Name', '').strip()
                agent_number = row.get('Agent Number', '').strip()
                
                if agent_name:
                    normalized = normalize_name(agent_name)
                    if normalized in missing_agents:
                        agent_details.append({
                            'Agent_Name': agent_name,
                            'Agent_Number': agent_number if agent_number and agent_number != '0' and agent_number != '#N/A' else '',
                            'Normalized_Name': normalized,
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
    print("=== FLEXIBLE AGENT COMPARISON ===")
    print("This script uses fuzzy matching to handle name variations like:")
    print("- 'JULIETH. SHARAMANZI' vs 'JULIETH SHARAMANZI C/0 PROCCO WAKALA'")
    print("- 'SKYSCAPE INTERNATIONAL 2' vs 'SKYSCAPE INTERNATIONAL'")
    print("- Names with T/A, C/O, LTD suffixes, etc.")
    print()
    
    # Read agents from MWALIMU-WAKALA.csv
    print("Reading MWALIMU-WAKALA.csv...")
    mwalimu_agents, mwalimu_numbers = read_csv_agents_flexible('MWALIMU-WAKALA.csv', 'Agent Name', 'Agent Number')
    print(f"Found {len(mwalimu_agents)} unique normalized agents in MWALIMU-WAKALA.csv")
    
    # Read agents from agent-from-cbs.csv
    print("Reading agent-from-cbs.csv...")
    cbs_agents, cbs_numbers = read_csv_agents_flexible('agent-from-cbs.csv', 'AGENTNAME', 'AGENTID')
    print(f"Found {len(cbs_agents)} unique normalized agents in agent-from-cbs.csv")
    
    # Find matches and non-matches
    matched_agents = set()
    match_details = []
    
    print("\nFinding matches using fuzzy matching...")
    
    for mwalimu_norm, mwalimu_orig in mwalimu_agents.items():
        # First try exact match
        if mwalimu_norm in cbs_agents:
            matched_agents.add(mwalimu_norm)
            match_details.append({
                'mwalimu_name': mwalimu_orig,
                'cbs_name': cbs_agents[mwalimu_norm],
                'match_type': 'exact',
                'score': 1.0
            })
        else:
            # Try fuzzy matching
            best_match, score = find_best_match(mwalimu_orig, list(cbs_agents.values()))
            if best_match:
                matched_agents.add(mwalimu_norm)
                match_details.append({
                    'mwalimu_name': mwalimu_orig,
                    'cbs_name': best_match,
                    'match_type': 'fuzzy',
                    'score': score
                })
    
    # Find agents not in CBS
    agents_not_in_cbs = set(mwalimu_agents.keys()) - matched_agents
    
    print(f"\nRESULTS:")
    print(f"- Total agents in MWALIMU-WAKALA: {len(mwalimu_agents)}")
    print(f"- Matched agents (exact + fuzzy): {len(matched_agents)}")
    print(f"- Agents NOT in CBS: {len(agents_not_in_cbs)}")
    
    # Save match details
    if match_details:
        print(f"\nSaving match details to 'agent_matches.csv'...")
        with open('agent_matches.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['mwalimu_name', 'cbs_name', 'match_type', 'score']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for match in match_details:
                writer.writerow(match)
        print(f"Saved {len(match_details)} matches")
    
    # Save missing agents
    if agents_not_in_cbs:
        print(f"\nGetting details of {len(agents_not_in_cbs)} missing agents...")
        missing_agent_details = get_agent_details_from_mwalimu('MWALIMU-WAKALA.csv', agents_not_in_cbs)
        
        output_filename = 'agents_missing_from_cbs_flexible.csv'
        print(f"Writing results to {output_filename}...")
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if missing_agent_details:
                fieldnames = missing_agent_details[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in missing_agent_details:
                    writer.writerow(agent)
                
                print(f"Successfully wrote {len(missing_agent_details)} missing agents to {output_filename}")
            else:
                print("No missing agent details found to write")
    else:
        print("All agents from MWALIMU-WAKALA.csv were found in agent-from-cbs.csv!")
    
    # Show some example matches
    print(f"\nSample matches found:")
    fuzzy_matches = [m for m in match_details if m['match_type'] == 'fuzzy'][:10]
    for match in fuzzy_matches:
        print(f"  MWALIMU: {match['mwalimu_name']}")
        print(f"  CBS:     {match['cbs_name']}")
        print(f"  Score:   {match['score']:.3f}")
        print()

if __name__ == "__main__":
    main()