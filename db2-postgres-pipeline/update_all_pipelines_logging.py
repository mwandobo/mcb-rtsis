#!/usr/bin/env python3
"""
Update all streaming pipelines to use professional connection logging
"""

import os
import re

def update_pipeline_file(filepath):
    """Update a single pipeline file to use log_connection parameter"""
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern 1: First connection in producer_thread (usually for count query)
    # Change: with self.db2_conn.get_connection() as conn:
    # To: with self.db2_conn.get_connection(log_connection=True) as conn:
    # But only for the FIRST occurrence in producer_thread
    
    # Find producer_thread method
    producer_match = re.search(r'def producer_thread\(self\):', content)
    if not producer_match:
        return False, "No producer_thread found"
    
    producer_start = producer_match.start()
    
    # Find first get_connection in producer_thread
    first_conn_pattern = r'(with self\.db2_conn\.get_connection\(\)) as conn:'
    first_match = re.search(first_conn_pattern, content[producer_start:])
    
    if first_match:
        # Replace first occurrence with log_connection=True
        match_pos = producer_start + first_match.start()
        content = (
            content[:match_pos] + 
            'with self.db2_conn.get_connection(log_connection=True) as conn:' +
            content[match_pos + len(first_match.group(0)) + len(' as conn:'):]
        )
    
    # Pattern 2: All other get_connection calls in producer_thread
    # Change remaining: with self.db2_conn.get_connection() as conn:
    # To: with self.db2_conn.get_connection(log_connection=False) as conn:
    
    # Find the next producer_thread or end of file
    next_method = re.search(r'\n    def \w+\(self', content[producer_start + 100:])
    if next_method:
        producer_end = producer_start + 100 + next_method.start()
    else:
        producer_end = len(content)
    
    # Replace remaining get_connection calls in producer_thread
    producer_section = content[producer_start:producer_end]
    updated_section = re.sub(
        r'with self\.db2_conn\.get_connection\(\) as conn:',
        'with self.db2_conn.get_connection(log_connection=False) as conn:',
        producer_section
    )
    
    content = content[:producer_start] + updated_section + content[producer_end:]
    
    if content != original_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True, "Updated"
    else:
        return False, "No changes needed"

def main():
    """Update all streaming pipeline files"""
    pipeline_dir = os.path.dirname(__file__)
    
    # Find all streaming pipeline files
    pipeline_files = [
        f for f in os.listdir(pipeline_dir)
        if f.endswith('_streaming_pipeline.py')
    ]
    
    print(f"Found {len(pipeline_files)} streaming pipeline files")
    print("=" * 60)
    
    updated_count = 0
    for filename in sorted(pipeline_files):
        filepath = os.path.join(pipeline_dir, filename)
        success, message = update_pipeline_file(filepath)
        
        status = "✓" if success else "○"
        print(f"{status} {filename}: {message}")
        
        if success:
            updated_count += 1
    
    print("=" * 60)
    print(f"Updated {updated_count}/{len(pipeline_files)} files")

if __name__ == "__main__":
    main()
