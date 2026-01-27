#!/usr/bin/env python3
"""
Create PostgreSQL table for ICBM transaction records
"""

import psycopg2
from config import Config

def create_icbm_transaction_table():
    """Create the icbmTransactions table in PostgreSQL"""
    config = Config()
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Drop table if exists
        cursor.execute("DROP TABLE IF EXISTS icbmTransactions CASCADE")
        
        # Create ICBM transactions table with all 8 fields using camelCase (quoted identifiers)
        create_table_query = """
        CREATE TABLE "icbmTransactions" (
            id SERIAL PRIMARY KEY,
            "reportingDate" DATE NOT NULL,
            "transactionDate" DATE,
            "lenderName" VARCHAR(255),
            "borrowerName" VARCHAR(255),
            "transactionType" VARCHAR(50) DEFAULT 'market',
            "tzsAmount" DECIMAL(15,2),
            tenure INTEGER,
            "interestRate" DECIMAL(8,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance using camelCase (quoted identifiers)
        cursor.execute('CREATE INDEX "idx_icbm_reporting_date" ON "icbmTransactions"("reportingDate")')
        cursor.execute('CREATE INDEX "idx_icbm_transaction_date" ON "icbmTransactions"("transactionDate")')
        cursor.execute('CREATE INDEX "idx_icbm_lender_name" ON "icbmTransactions"("lenderName")')
        cursor.execute('CREATE INDEX "idx_icbm_borrower_name" ON "icbmTransactions"("borrowerName")')
        cursor.execute('CREATE INDEX "idx_icbm_transaction_type" ON "icbmTransactions"("transactionType")')
        cursor.execute('CREATE INDEX "idx_icbm_created_at" ON "icbmTransactions"(created_at)')
        
        conn.commit()
        print("ICBM Transactions table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'icbmTransactions' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\nTable structure ({len(columns)} columns):")
        for col in columns:
            nullable = 'NULL' if col[2] == 'YES' else 'NOT NULL'
            default = f" DEFAULT {col[3]}" if col[3] else ""
            print(f"  {col[0]}: {col[1]} ({nullable}){default}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error creating ICBM transactions table: {e}")
        raise

if __name__ == "__main__":
    create_icbm_transaction_table()