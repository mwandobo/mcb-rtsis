#!/usr/bin/env python3
"""
Create PostgreSQL table for IBCM transactions records
Based on ibcm_transactions-v1.sql query structure
"""

import sys
import os
import psycopg2

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
        
        cursor = conn.cursor()
        
        # Drop table if exists
        cursor.execute("DROP TABLE IF EXISTS \"ibcmTransactions\" CASCADE")
        print("Dropped existing IBCM transactions table")
        
        # Create IBCM transactions table based on ibcm_transactions-v1.sql structure
        create_table_query = """
        CREATE TABLE "ibcmTransactions" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12) NOT NULL,
            "transactionDate" VARCHAR(12) NOT NULL,
            "lenderName" VARCHAR(255) NOT NULL,
            "borrowerName" VARCHAR(255) NOT NULL,
            "transactionType" VARCHAR(50),
            "tzsAmount" DECIMAL(18,2),
            tenure INTEGER,
            "interestRate" DECIMAL(8,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_ibcm_transactions_reporting_date ON \"ibcmTransactions\"(\"reportingDate\")")
        cursor.execute("CREATE INDEX idx_ibcm_transactions_transaction_date ON \"ibcmTransactions\"(\"transactionDate\")")
        cursor.execute("CREATE INDEX idx_ibcm_transactions_lender ON \"ibcmTransactions\"(\"lenderName\")")
        cursor.execute("CREATE INDEX idx_ibcm_transactions_borrower ON \"ibcmTransactions\"(\"borrowerName\")")
        cursor.execute("CREATE INDEX idx_ibcm_transactions_type ON \"ibcmTransactions\"(\"transactionType\")")
        cursor.execute("CREATE INDEX idx_ibcm_transactions_created_at ON \"ibcmTransactions\"(created_at)")
        
        conn.commit()
        print("IBCM transactions table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'ibcmTransactions' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\nIBCM transactions table created with {len(columns)} columns:")
        for col in columns:
            print(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating IBCM transactions table: {e}")
        raise


if __name__ == "__main__":
    create_ibcm_transactions_table()