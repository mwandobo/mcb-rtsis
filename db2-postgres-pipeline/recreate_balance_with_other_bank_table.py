#!/usr/bin/env python3
"""
Recreate balanceWithOtherBank table without id column
"""

import psycopg2
from config import Config

def recreate_balance_table():
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
        
        # Drop existing table
        print("Dropping existing balanceWithOtherBank table...")
        cursor.execute('DROP TABLE IF EXISTS "balanceWithOtherBank" CASCADE;')
        conn.commit()
        print("✓ Table dropped")
        
        # Create new table without id
        print("Creating new balanceWithOtherBank table...")
        create_table_sql = """
        CREATE TABLE "balanceWithOtherBank" (
            "reportingDate" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "accountName" VARCHAR(200),
            "bankCode" VARCHAR(50),
            "country" VARCHAR(50),
            "relationshipType" VARCHAR(100),
            "accountType" VARCHAR(50),
            "subAccountType" VARCHAR(50),
            "currency" VARCHAR(10),
            "orgAmount" DECIMAL(18,2),
            "usdAmount" DECIMAL(18,2),
            "tzsAmount" DECIMAL(18,2),
            "transactionDate" VARCHAR(50),
            "pastDueDays" INTEGER,
            "allowanceProbableLoss" DECIMAL(18,2),
            "botProvision" DECIMAL(18,2),
            "assetsClassificationCategory" VARCHAR(50),
            "contractDate" VARCHAR(50),
            "maturityDate" VARCHAR(50),
            "externalRatingCorrespondentBank" VARCHAR(200),
            "gradesUnratedBanks" VARCHAR(100)
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        print("✓ Table created successfully")
        
        # Verify table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithOtherBank'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"\nTable structure ({len(columns)} columns):")
        for col_name, col_type in columns:
            print(f"  - {col_name}: {col_type}")
        
        cursor.close()
        conn.close()
        
        print("\n✓ balanceWithOtherBank table recreated successfully!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise

if __name__ == "__main__":
    recreate_balance_table()
