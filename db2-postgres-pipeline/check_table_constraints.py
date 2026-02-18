#!/usr/bin/env python3
"""
Check and remove constraints from loan information table
"""

import psycopg2
from config import Config

def check_and_remove_constraints():
    """Check and remove constraints from loan information table"""
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        cursor = conn.cursor()
        
        # Check existing constraints
        cursor.execute("""
            SELECT conname, contype 
            FROM pg_constraint 
            WHERE conrelid = (
                SELECT oid FROM pg_class WHERE relname = 'loanInformation'
            )
        """)
        constraints = cursor.fetchall()
        
        print("Current constraints on loanInformation table:")
        for constraint_name, constraint_type in constraints:
            constraint_types = {
                'p': 'PRIMARY KEY',
                'u': 'UNIQUE',
                'f': 'FOREIGN KEY',
                'c': 'CHECK'
            }
            print(f"  - {constraint_name}: {constraint_types.get(constraint_type, constraint_type)}")
        
        # Drop unique constraint on accountNumber if it exists
        for constraint_name, constraint_type in constraints:
            if constraint_type == 'u':  # UNIQUE constraint
                print(f"\nDropping UNIQUE constraint: {constraint_name}")
                cursor.execute(f'ALTER TABLE "loanInformation" DROP CONSTRAINT "{constraint_name}"')
                conn.commit()
                print(f"✅ Dropped constraint: {constraint_name}")
        
        # Check if there are any indexes on accountNumber
        cursor.execute("""
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE tablename = 'loanInformation' 
            AND indexdef LIKE '%accountNumber%'
        """)
        indexes = cursor.fetchall()
        
        if indexes:
            print("\nIndexes on accountNumber:")
            for index_name, index_def in indexes:
                print(f"  - {index_name}: {index_def}")
                if 'UNIQUE' in index_def.upper():
                    print(f"    Dropping UNIQUE index: {index_name}")
                    cursor.execute(f'DROP INDEX IF EXISTS "{index_name}"')
                    conn.commit()
                    print(f"    ✅ Dropped index: {index_name}")
        
        conn.close()
        print("\n✅ Table is now ready to accept duplicate accountNumber values")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_and_remove_constraints()