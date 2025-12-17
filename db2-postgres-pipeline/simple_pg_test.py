#!/usr/bin/env python3
"""
Simple PostgreSQL Connection Test
Quick test to verify PostgreSQL connectivity
"""

import os
import psycopg2
from dotenv import load_dotenv

def test_connection():
    """Simple PostgreSQL connection test"""
    
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    host = os.getenv('PG_HOST', 'localhost')
    port = int(os.getenv('PG_PORT', 5432))
    database = os.getenv('PG_DATABASE', 'postgres')
    user = os.getenv('PG_USER', 'postgres')
    password = os.getenv('PG_PASSWORD', 'postgres')
    
    print(f"Testing connection to PostgreSQL...")
    print(f"Host: {host}:{port}")
    print(f"Database: {database}")
    print(f"User: {user}")
    print("-" * 50)
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )
        
        # Execute a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT version(), current_database(), current_user, NOW();")
        result = cursor.fetchone()
        
        print("✅ CONNECTION SUCCESSFUL!")
        print(f"PostgreSQL Version: {result[0].split(',')[0]}")
        print(f"Database: {result[1]}")
        print(f"User: {result[2]}")
        print(f"Server Time: {result[3]}")
        
        # Test a simple table count query
        try:
            cursor.execute("""
                SELECT schemaname, tablename, n_tup_ins as row_count
                FROM pg_stat_user_tables 
                ORDER BY n_tup_ins DESC 
                LIMIT 5;
            """)
            
            tables = cursor.fetchall()
            if tables:
                print("\nTop 5 tables by row count:")
                for schema, table, count in tables:
                    print(f"  {schema}.{table}: {count:,} rows")
            else:
                print("\nNo user tables found or no statistics available.")
                
        except Exception as e:
            print(f"\nNote: Could not get table statistics: {e}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ CONNECTION FAILED!")
        print(f"Error: {e}")
        print("\nPossible issues:")
        print("- Check if PostgreSQL server is running")
        print("- Verify host and port are correct")
        print("- Check username and password")
        print("- Ensure database exists")
        print("- Check firewall settings")
        return False
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR!")
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    exit(0 if success else 1)