#!/usr/bin/env python3
"""
Validate that personal_data_streaming_pipeline fields match correctly
"""

import re
from dataclasses import fields
from personal_data_streaming_pipeline import PersonalDataRecord

def validate_fields():
    """Validate field counts and names"""
    
    print("=" * 70)
    print("Personal Data Pipeline Field Validation")
    print("=" * 70)
    
    # 1. Get dataclass fields
    dataclass_fields = [f.name for f in fields(PersonalDataRecord)]
    print(f"\n1. Dataclass fields: {len(dataclass_fields)}")
    for i, field in enumerate(dataclass_fields, 1):
        print(f"   {i:2d}. {field}")
    
    # 2. Read the insert_to_postgres method from the file
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_file = os.path.join(script_dir, 'personal_data_streaming_pipeline.py')
    with open(pipeline_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract INSERT statement field names
    insert_match = re.search(
        r'INSERT INTO "personalData" \((.*?)\) VALUES',
        content,
        re.DOTALL
    )
    
    if insert_match:
        insert_fields_text = insert_match.group(1)
        # Extract field names (handle quoted and unquoted)
        insert_fields = re.findall(r'["\']?(\w+)["\']?', insert_fields_text)
        insert_fields = [f for f in insert_fields if f and f not in ('INTO', 'personalData')]
        
        print(f"\n2. INSERT statement fields: {len(insert_fields)}")
        for i, field in enumerate(insert_fields, 1):
            print(f"   {i:2d}. {field}")
    
    # 3. Count placeholders
    values_match = re.search(
        r'VALUES \((.*?)\)',
        content,
        re.DOTALL
    )
    
    if values_match:
        placeholders = values_match.group(1).count('%s')
        print(f"\n3. Placeholders (%s) in VALUES: {placeholders}")
    
    # 4. Extract the tuple being passed to cursor.execute
    execute_match = re.search(
        r'cursor\.execute\(\s*insert_sql,\s*\((.*?)\),?\s*\)',
        content,
        re.DOTALL
    )
    
    if execute_match:
        tuple_text = execute_match.group(1)
        # Count record.field references
        tuple_fields = re.findall(r'record\.(\w+)', tuple_text)
        print(f"\n4. Values in execute tuple: {len(tuple_fields)}")
        for i, field in enumerate(tuple_fields, 1):
            print(f"   {i:2d}. record.{field}")
    
    # 5. Validation
    print("\n" + "=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)
    
    all_valid = True
    
    # Check counts match
    if len(dataclass_fields) == len(insert_fields) == placeholders == len(tuple_fields):
        print(f"✅ All counts match: {len(dataclass_fields)} fields")
    else:
        print(f"❌ Count mismatch!")
        print(f"   Dataclass: {len(dataclass_fields)}")
        print(f"   INSERT fields: {len(insert_fields)}")
        print(f"   Placeholders: {placeholders}")
        print(f"   Execute tuple: {len(tuple_fields)}")
        all_valid = False
    
    # Check field names match
    print(f"\n📋 Field name comparison:")
    mismatches = []
    for i, (dc_field, ins_field, tup_field) in enumerate(zip(dataclass_fields, insert_fields, tuple_fields), 1):
        if dc_field == ins_field == tup_field:
            if i <= 5 or i > len(dataclass_fields) - 5:  # Show first and last 5
                print(f"   {i:2d}. ✅ {dc_field}")
        else:
            print(f"   {i:2d}. ❌ Mismatch: dataclass={dc_field}, INSERT={ins_field}, tuple={tup_field}")
            mismatches.append((i, dc_field, ins_field, tup_field))
            all_valid = False
    
    if len(dataclass_fields) > 10:
        print(f"   ... ({len(dataclass_fields) - 10} middle fields not shown)")
    
    if mismatches:
        print(f"\n❌ Found {len(mismatches)} field mismatches")
        for idx, dc, ins, tup in mismatches:
            print(f"   Position {idx}: dataclass='{dc}', INSERT='{ins}', tuple='{tup}'")
    else:
        print(f"\n✅ All field names match in order")
    
    print("\n" + "=" * 70)
    if all_valid:
        print("✅ VALIDATION PASSED - Pipeline is ready to run!")
    else:
        print("❌ VALIDATION FAILED - Fix issues before running pipeline")
    print("=" * 70)
    
    return all_valid

if __name__ == "__main__":
    validate_fields()
