#!/usr/bin/env python3
"""
Script to convert PostgreSQL schema to use quoted identifiers for camelCase preservation
"""

import re

def quote_identifiers(sql_content):
    """Add quotes around identifiers to preserve camelCase in PostgreSQL"""
    
    # Quote table names in CREATE TABLE statements
    sql_content = re.sub(
        r'CREATE TABLE IF NOT EXISTS (\w+) \(',
        r'CREATE TABLE IF NOT EXISTS "\1" (',
        sql_content
    )
    
    # Quote column names (but not data types, constraints, etc.)
    # This regex looks for column definitions: word followed by data type
    sql_content = re.sub(
        r'^\s*(\w+)\s+(VARCHAR|INTEGER|DECIMAL|DATE|TIMESTAMP|CHAR)',
        r'    "\1" \2',
        sql_content,
        flags=re.MULTILINE
    )
    
    # Quote column names in PRIMARY KEY constraints
    sql_content = re.sub(
        r'PRIMARY KEY \(([^)]+)\)',
        lambda m: 'PRIMARY KEY (' + ', '.join(f'"{col.strip()}"' for col in m.group(1).split(',')) + ')',
        sql_content
    )
    
    # Quote column names in CREATE INDEX statements
    sql_content = re.sub(
        r'CREATE INDEX[^(]+ON (\w+)\(([^)]+)\)',
        lambda m: m.group(0).replace(m.group(1), f'"{m.group(1)}"').replace(
            m.group(2), ', '.join(f'"{col.strip()}"' for col in m.group(2).split(','))
        ),
        sql_content
    )
    
    return sql_content

def main():
    # Read the current schema
    with open('sql/postgres-schema.sql', 'r') as f:
        content = f.read()
    
    # Apply quoting
    quoted_content = quote_identifiers(content)
    
    # Write back
    with open('sql/postgres-schema-quoted.sql', 'w') as f:
        f.write(quoted_content)
    
    print("Created postgres-schema-quoted.sql with proper camelCase quoting")

if __name__ == "__main__":
    main()