#!/usr/bin/env python3
"""
Investigate why THOMAS E NGULIKA appears as missing when he should be in CBS
"""

import csv

def search_cbs_for_thomas():
    """Search CBS file thoroughly for THOMAS E NGULIKA or similar"""
    print("=== SEARCHING CBS FILE FOR THOMAS E NGULIKA ===")
    
    thomas_matches = []
    ngulika_matches = []
    terminal_id_matches = []
    
    try:
        with open('agent-from-cbs.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for i, row in enumerate(reader, 1):
                agent_id = row.get('AGENTID', '').strip()
                agent_name = row.get('AGENTNAME', '').strip().upper()
                
                # Search for THOMAS
                if 'THOMAS' in agent_name:
                    thomas_matches.append({
                        'row': i,
                        'AGENTID': agent_id,
                        'AGENTNAME': agent_name
                    })
                
                # Search for NGULIKA
                if 'NGULIKA' in agent_name:
                    ngulika_matches.append({
                        'row': i,
                        'AGENTID': agent_id,
                        'AGENTNAME': agent_name
                    })
                
                # Search for Terminal ID patterns
                if 'I0027469' in agent_id or 'PI0027469' in agent_id or '0027469' in agent_id:
                    terminal_id_matches.append({
                        'row': i,
                        'AGENTID': agent_id,
                        'AGENTNAME': agent_name
                    })
                    
    except Exception as e:
        print(f"Error reading CBS file: {e}")
        return
    
    print(f"Found {len(thomas_matches)} agents with 'THOMAS' in name:")
    for match in thomas_matches:
        print(f"  Row {match['row']}: {match['AGENTID']} - {match['AGENTNAME']}")
    
    print(f"\nFound {len(ngulika_matches)} agents with 'NGULIKA' in name:")
    for match in ngulika_matches:
        print(f"  Row {match['row']}: {match['AGENTID']} - {match['AGENTNAME']}")
    
    print(f"\nFound {len(terminal_id_matches)} agents with Terminal ID pattern:")
    for match in terminal_id_matches:
        print(f"  Row {match['row']}: {match['AGENTID']} - {match['AGENTNAME']}")

def check_mwalimu_thomas():
    """Check THOMAS E NGULIKA details in MWALIMU file"""
    print("\n=== THOMAS E NGULIKA IN MWALIMU FILE ===")
    
    try:
        with open('mwalimu_unique_agents.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for i, row in enumerate(reader, 1):
                agent_name = row.get('Agent Name', '').strip()
                
                if 'THOMAS E NGULIKA' in agent_name.upper():
                    print(f"Found in MWALIMU at row {i}:")
                    print(f"  Terminal ID: {row.get('Terminal ID', '')}")
                    print(f"  Agent Number: {row.get('Agent Number', '')}")
                    print(f"  Agent Name: {row.get('Agent Name', '')}")
                    print(f"  Region: {row.get('Region', '')}")
                    print(f"  District: {row.get('District', '')}")
                    print(f"  Contact: {row.get('contact', '')}")
                    print(f"  Business: {row.get('Current Business', '')}")
                    
    except Exception as e:
        print(f"Error reading MWALIMU file: {e}")

def check_original_mwalimu():
    """Check if THOMAS E NGULIKA is in the original MWALIMU-WAKALA.csv"""
    print("\n=== CHECKING ORIGINAL MWALIMU-WAKALA.CSV ===")
    
    try:
        with open('MWALIMU-WAKALA.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            thomas_count = 0
            for i, row in enumerate(reader, 1):
                agent_name = row.get('Agent Name', '').strip()
                
                if 'THOMAS E NGULIKA' in agent_name.upper():
                    thomas_count += 1
                    print(f"Found in original MWALIMU at row {i} (occurrence #{thomas_count}):")
                    print(f"  Terminal ID: {row.get('Terminal ID', '')}")
                    print(f"  Agent Number: {row.get('Agent Number', '')}")
                    print(f"  Agent Name: {row.get('Agent Name', '')}")
                    print(f"  Region: {row.get('Region', '')}")
                    print(f"  District: {row.get('District', '')}")
                    
            print(f"\nTotal occurrences of THOMAS E NGULIKA in original file: {thomas_count}")
                    
    except Exception as e:
        print(f"Error reading original MWALIMU file: {e}")

def main():
    search_cbs_for_thomas()
    check_mwalimu_thomas()
    check_original_mwalimu()

if __name__ == "__main__":
    main()