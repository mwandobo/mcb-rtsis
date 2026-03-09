#!/usr/bin/env python3
"""
Create Account Information table in PostgreSQL
Based on account-information.sql query
"""

import psycopg2
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_account_information_table():
    """Create the accountInformation table in PostgreSQL"""
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
        print("Dropping existing accountInformation table if exists...")
        cursor.execute('DROP TABLE IF EXISTS "accountInformation" CASCADE')
        
        # Create table
        print("Creating accountInformation table...")
        create_table_sql = """
        CREATE TABLE "accountInformation" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12) NOT NULL,
            "customerIdentificationNumber" VARCHAR(20),
            "accountNumber" VARCHAR(50) NOT NULL,
            "accountProductCode" VARCHAR(50),
            "accountOperationStatus" VARCHAR(50),
            "customerType" VARCHAR(50),
            "smrCode" VARCHAR(50),
            status VARCHAR(10),
            "orgAccountBalance" DECIMAL(18, 2),
            "usdAccountBalance" DECIMAL(18, 2),
            "tzsAccountBalance" DECIMAL(18, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        print("Creating indexes...")
        cursor.execute('CREATE INDEX idx_ai_account_number ON "accountInformation"("accountNumber")')
        cursor.execute('CREATE INDEX idx_ai_customer_id ON "accountInformation"("customerIdentificationNumber")')
        cursor.execute('CREATE INDEX idx_ai_product_code ON "accountInformation"("accountProductCode")')
        cursor.execute('CREATE INDEX idx_ai_reporting_date ON "accountInformation"("reportingDate")')
        
        # Create unique constraint on accountNumber to prevent duplicates
        cursor.execute('CREATE UNIQUE INDEX idx_ai_account_unique ON "accountInformation"("accountNumber")')
        
        conn.commit()
        
        print("✅ Table 'accountInformation' created successfully!")
        print("✅ Indexes created successfully!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise

if __name__ == "__main__":
    create_account_information_table()
