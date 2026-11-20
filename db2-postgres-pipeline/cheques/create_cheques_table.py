#!/usr/bin/env python3
"""
Create PostgreSQL table for cheques data
"""

import psycopg2
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_cheques_table():
    """Create the cheques table in PostgreSQL"""
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
        
        # Create table SQL - using VARCHAR for all date fields to match data format
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS "chequeClearing" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "chequeNumber" VARCHAR(100),
            "issuerName" VARCHAR(255),
            "issuerBankerCode" VARCHAR(50),
            "payeeName" VARCHAR(255),
            "payeeAccountNumber" VARCHAR(100),
            "chequeDate" VARCHAR(50),
            "transactionDate" VARCHAR(50),
            "settlementDate" VARCHAR(50),
            "allowanceProbableLoss" VARCHAR(50),
            "botProvision" VARCHAR(50),
            "currency" VARCHAR(10),
            "orgAmountOpening" VARCHAR(50),
            "usdAmountOpening" VARCHAR(50),
            "tzsAmountOpening" VARCHAR(50),
            "orgAmountPayment" VARCHAR(50),
            "usdAmountPayment" VARCHAR(50),
            "tzsAmountPayment" VARCHAR(50),
            "orgAmountBalance" VARCHAR(50),
            "usdAmountBalance" VARCHAR(50),
            "tzsAmountBalance" VARCHAR(50),
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_sql)
        
        # Create non-unique index for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_chequeclearing_number
            ON "chequeClearing" ("chequeNumber")
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_chequeclearing_date
            ON "chequeClearing" ("transactionDate")
        """)
        
        conn.commit()
        print("✅ ChequeClearing table created successfully")
        print("✅ Performance indexes created (chequeNumber, transactionDate)")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'chequeClearing'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\n📋 Table structure ({len(columns)} columns):")
        for col in columns:
            nullable = "NULL" if col[2] == "YES" else "NOT NULL"
            print(f"  - {col[0]}: {col[1]} {nullable}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating chequeClearing table: {e}")
        sys.exit(1)

if __name__ == "__main__":
    create_cheques_table()