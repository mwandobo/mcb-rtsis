#!/usr/bin/env python3
"""
Check IBCM Transactions table structure and data
"""
import psycopg2
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def check_table():
    """Check the IBCM transactions table"""
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
        
        with conn.cursor() as cursor:
            # Check table structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'ibcmTransactions' 
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            print("✅ Table columns (camelCase):")
            for col_name, data_type, is_nullable in columns:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"  {col_name}: {data_type} {nullable}")
            
            # Check row count
            cursor.execute('SELECT COUNT(*) FROM "ibcmTransactions"')
            count = cursor.fetchone()[0]
            print(f"\n✅ Row count: {count}")
            
            # Show sample data if any exists
            if count > 0:
                cursor.execute('SELECT * FROM "ibcmTransactions" LIMIT 3')
                rows = cursor.fetchall()
                print(f"\n✅ Sample data (first 3 rows):")
                for i, row in enumerate(rows, 1):
                    print(f"  Row {i}: {row}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking table: {e}")

if __name__ == "__main__":
    check_table()