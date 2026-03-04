#!/usr/bin/env python3
"""
Create IBCM Transactions table in PostgreSQL
"""
import psycopg2
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_ibcm_transactions_table():
    """Create the IBCM transactions table in PostgreSQL"""
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
        
        # Read SQL file
        sql_file_path = os.path.join(os.path.dirname(__file__), 'create_ibcm_transactions_table.sql')
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
        
        # Execute SQL
        with conn.cursor() as cursor:
            cursor.execute(sql_script)
            conn.commit()
            
        print("✅ IBCM transactions table created successfully!")
        print("Table: \"ibcmTransactions\"")
        print("Indexes: Created for performance optimization")
        print("Constraints: Unique constraint on (\"transactionDate\", \"lenderName\", \"borrowerName\")")
        
        # Verify table creation
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'ibcmTransactions' 
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            print("\nTable structure:")
            for col_name, data_type, is_nullable in columns:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"  {col_name}: {data_type} {nullable}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating IBCM transactions table: {e}")
        sys.exit(1)

if __name__ == "__main__":
    create_ibcm_transactions_table()