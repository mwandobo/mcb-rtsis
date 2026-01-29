#!/usr/bin/env python3
"""
Create two filtered lists from mwalimu_unique_agents.csv:
1. Agents with complete location info (Region, District, Physical Location)
2. Agents missing TIN and Business License
"""

import csv

def create_filtered_lists():
    """Create two filtered CSV files based on specified criteria"""
    
    # Read the unique agents file
    try:
        with open('mwalimu_unique_agents.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            all_agents = list(reader)
    except Exception as e:
        print(f"Error reading mwalimu_unique_agents.csv: {e}")
        return
    
    print(f"Total unique agents: {len(all_agents)}")
    
    # List 1: Agents with complete location info (Region, District, Physical Location)
    complete_location_agents = []
    
    for agent in all_agents:
        region = agent.get('Region', '').strip()
        district = agent.get('District', '').strip()
        physical_location = agent.get('Physical Location And Postal Address', '').strip()
        
        # Check if all three location fields have data
        if region and district and physical_location:
            complete_location_agents.append(agent)
    
    print(f"Agents with complete location info: {len(complete_location_agents)}")
    
    # List 2: Agents missing TIN and Business License
    missing_tin_license_agents = []
    
    for agent in all_agents:
        tin = agent.get('Tin', '').strip()
        business_license = agent.get('Business Licence No. Issuer And Date', '').strip()
        
        # Check if both TIN and Business License are missing or empty
        if not tin and not business_license:
            missing_tin_license_agents.append(agent)
    
    print(f"Agents missing both TIN and Business License: {len(missing_tin_license_agents)}")
    
    # Write List 1: Complete Location Info
    output_file1 = 'agents_with_complete_location.csv'
    try:
        with open(output_file1, 'w', newline='', encoding='utf-8') as csvfile:
            if complete_location_agents:
                fieldnames = complete_location_agents[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in complete_location_agents:
                    writer.writerow(agent)
                
                print(f"✓ Successfully wrote {len(complete_location_agents)} agents to {output_file1}")
            else:
                print(f"No agents with complete location info found")
    except Exception as e:
        print(f"Error writing {output_file1}: {e}")
    
    # Write List 2: Missing TIN and Business License
    output_file2 = 'agents_missing_tin_and_license.csv'
    try:
        with open(output_file2, 'w', newline='', encoding='utf-8') as csvfile:
            if missing_tin_license_agents:
                fieldnames = missing_tin_license_agents[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for agent in missing_tin_license_agents:
                    writer.writerow(agent)
                
                print(f"✓ Successfully wrote {len(missing_tin_license_agents)} agents to {output_file2}")
            else:
                print(f"No agents missing both TIN and Business License found")
    except Exception as e:
        print(f"Error writing {output_file2}: {e}")
    
    # Summary statistics
    print(f"\n=== SUMMARY ===")
    print(f"Total unique agents: {len(all_agents)}")
    print(f"Agents with complete location info: {len(complete_location_agents)} ({len(complete_location_agents)/len(all_agents)*100:.1f}%)")
    print(f"Agents missing TIN and Business License: {len(missing_tin_license_agents)} ({len(missing_tin_license_agents)/len(all_agents)*100:.1f}%)")
    
    # Additional breakdown for missing data
    tin_missing = sum(1 for agent in all_agents if not agent.get('Tin', '').strip())
    license_missing = sum(1 for agent in all_agents if not agent.get('Business Licence No. Issuer And Date', '').strip())
    
    print(f"\nAdditional statistics:")
    print(f"Agents missing TIN: {tin_missing} ({tin_missing/len(all_agents)*100:.1f}%)")
    print(f"Agents missing Business License: {license_missing} ({license_missing/len(all_agents)*100:.1f}%)")

if __name__ == "__main__":
    create_filtered_lists()