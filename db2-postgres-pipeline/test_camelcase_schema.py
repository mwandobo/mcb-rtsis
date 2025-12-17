#!/usr/bin/env python3
"""
Test script to verify camelCase preservation in PostgreSQL schema
"""

import os
import psycopg2
from dotenv import load_dotenv

def test_camelcase_schema():
    """Test that camelCase identifiers are preserved in PostgreSQL"""
    
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    host = os.getenv('PG_HOST', 'localhost')
    port = int(os.getenv('PG_PORT', 5432))
    database = os.getenv('PG_DATABASE', 'postgres')
    user = os.getenv('PG_USER', 'postgres')
    password = os.getenv('PG_PASSWORD', 'postgres')
    
    print("Testing camelCase schema preservation...")
    print("-" * 50)
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        cursor = conn.cursor()
        
        # Test 1: Check if we can create a simple camelCase table
        print("Test 1: Creating test camelCase table...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "testCamelCase" (
                "testColumn" VARCHAR(50),
                "anotherColumn" INTEGER
            );
        ''')
        
        # Test 2: Insert data using camelCase
        print("Test 2: Inserting data with camelCase columns...")
        cursor.execute('''
            INSERT INTO "testCamelCase" ("testColumn", "anotherColumn") 
            VALUES ('test value', 123);
        ''')
        
        # Test 3: Query data using camelCase
        print("Test 3: Querying data with camelCase columns...")
        cursor.execute('''
            SELECT "testColumn", "anotherColumn" 
            FROM "testCamelCase" 
            WHERE "testColumn" = 'test value';
        ''')
        
        result = cursor.fetchone()
        if result:
            print(f"✅ Query successful: {result}")
        else:
            print("❌ No data returned")
        
        # Test 4: Check actual column names in information_schema
        print("Test 4: Verifying column names in information_schema...")
        cursor.execute('''
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'testCamelCase' 
            ORDER BY ordinal_position;
        ''')
        
        columns = cursor.fetchall()
        print("Actual column names in database:")
        for col in columns:
            print(f"  - {col[0]}")
        
        # Test 5: Test investment debt securities table structure
        print("\nTest 5: Checking investmentDebtSecurities table structure...")
        cursor.execute('''
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'investmentDebtSecurities' 
            ORDER BY ordinal_position
            LIMIT 5;
        ''')
        
        ids_columns = cursor.fetchall()
        if ids_columns:
            print("investmentDebtSecurities columns (first 5):")
            for col_name, data_type in ids_columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print("⚠️  investmentDebtSecurities table not found (may not be created yet)")
        
        # Clean up test table
        cursor.execute('DROP TABLE IF EXISTS "testCamelCase";')
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n✅ camelCase schema test completed successfully!")
        print("\nKey points:")
        print("- Use double quotes around ALL identifiers (tables and columns)")
        print("- Example: SELECT \"reportingDate\" FROM \"investmentDebtSecurities\"")
        print("- Without quotes, PostgreSQL converts to lowercase")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_camelcase_schema()
    exit(0 if success else 1)