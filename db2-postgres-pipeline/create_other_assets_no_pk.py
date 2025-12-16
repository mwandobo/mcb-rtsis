#!/usr/bin/env python3
"""
Create other_assets table without primary key constraint
"""

import psycopg2
from config import Config

def create_other_assets_table_no_pk():
    """Create other_assets table without primary key"""
    config = Config()
    
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Create other_assets table without primary key constraint
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS other_assets (
        "reportingDate" TIMESTAMP,
        "assetType" VARCHAR(50),
        "transactionDate" DATE,
        "maturityDate" DATE,
        "debtorName" VARCHAR(200),
        "debtorCountry" VARCHAR(50),
        currency VARCHAR(10),
        "orgAmount" DECIMAL(15,2),
        "usdAmount" DECIMAL(15,2),
        "tzsAmount" DECIMAL(15,2),
        "sectorSnaClassification" VARCHAR(100),
        "pastDueDays" INTEGER,
        "assetClassificationCategory" INTEGER,
        "allowanceProbableLoss" DECIMAL(15,2),
        "botProvision" DECIMAL(15,2)
    )
    '''
    
    cursor.execute(create_table_sql)
    conn.commit()
    
    cursor.close()
    conn.close()
    
    print("✅ other_assets table created WITHOUT primary key constraint")
    print("   Now all records can be inserted without conflicts")

if __name__ == "__main__":
    create_other_assets_table_no_pk()