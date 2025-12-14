#!/usr/bin/env python3
"""
Create missing balance_with_other_bank table
"""

import psycopg2
from config import Config

def create_missing_table():
    config = Config()
    
    # SQL to create the missing table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS balance_with_other_bank (
        "reportingDate" TIMESTAMP,
        "accountNumber" VARCHAR(50),
        "accountName" VARCHAR(200),
        "bankCode" VARCHAR(20),
        country VARCHAR(50),
        "relationshipType" VARCHAR(50),
        "accountType" VARCHAR(50),
        "subAccountType" VARCHAR(50),
        currency VARCHAR(10),
        "orgAmount" DECIMAL(15,2),
        "usdAmount" DECIMAL(15,2),
        "tzsAmount" DECIMAL(15,2),
        "transactionDate" DATE,
        "pastDueDays" INTEGER,
        "allowanceProbableLoss" DECIMAL(15,2),
        "botProvision" DECIMAL(15,2),
        "assetsClassificationCategory" VARCHAR(50),
        "contractDate" DATE,
        "maturityDate" DATE,
        "externalRatingCorrespondentBank" VARCHAR(100),
        "gradesUnratedBanks" VARCHAR(50),
        PRIMARY KEY ("accountNumber", "transactionDate")
    );
    """
    
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
        
        # Create the table
        cursor.execute(create_table_sql)
        conn.commit()
        
        print("✅ Successfully created balance_with_other_bank table")
        
        # Verify table exists
        cursor.execute("SELECT COUNT(*) FROM balance_with_other_bank")
        count = cursor.fetchone()[0]
        print(f"✅ Table verified - current record count: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise

if __name__ == "__main__":
    create_missing_table()