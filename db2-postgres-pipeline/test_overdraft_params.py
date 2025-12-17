#!/usr/bin/env python3
"""
Test overdraft parameter count
"""

from processors.overdraft_processor import OverdraftProcessor

def test_parameter_count():
    """Test parameter count in overdraft query"""
    processor = OverdraftProcessor()
    query = processor.get_upsert_query()
    
    # Count %s placeholders
    placeholder_count = query.count('%s')
    print(f"Placeholders in query: {placeholder_count}")
    
    # Count columns in INSERT
    insert_part = query.split('INSERT INTO overdraft (')[1].split(') VALUES')[0]
    columns = [col.strip().strip('"') for col in insert_part.split(',')]
    print(f"Columns in INSERT: {len(columns)}")
    
    print("\nColumns:")
    for i, col in enumerate(columns, 1):
        print(f"  {i:2d}. {col}")

if __name__ == "__main__":
    test_parameter_count()