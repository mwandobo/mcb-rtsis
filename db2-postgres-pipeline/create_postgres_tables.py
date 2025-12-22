#!/usr/bin/env python3
"""
Script to create PostgreSQL tables with serial ID primary keys
"""

import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_tables():
    """Create all PostgreSQL tables with serial ID primary keys"""
    
    # Database connection parameters
    conn_params = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': int(os.getenv('PG_PORT', '5432')),
        'database': os.getenv('PG_DATABASE', 'bank_data'),
        'user': os.getenv('PG_USER', 'postgres'),
        'password': os.getenv('PG_PASSWORD', 'postgres')
    }
    
    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}")
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Read and execute the schema file
        schema_file = 'sql/postgres-schema.sql'
        print(f"Reading schema file: {schema_file}")
        
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        print("Executing schema creation...")
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✅ Successfully created all tables with serial ID primary keys!")
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print(f"\n📋 Created {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        # Check for serial columns
        cursor.execute("""
            SELECT table_name, column_name, data_type, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND column_name = 'id'
            ORDER BY table_name;
        """)
        
        id_columns = cursor.fetchall()
        print(f"\n🔑 Tables with serial ID primary keys: {len(id_columns)}")
        for table, column, data_type, default in id_columns:
            print(f"  - {table}.{column} ({data_type}) - {default}")
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error: {e}")
        return False
    except FileNotFoundError:
        print(f"❌ Schema file not found: {schema_file}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
    return True

if __name__ == "__main__":
    print("🚀 Creating PostgreSQL tables with serial ID primary keys...")
    success = create_tables()
    
    if success:
        print("\n✅ Table creation completed successfully!")
    else:
        print("\n❌ Table creation failed!")
        exit(1)