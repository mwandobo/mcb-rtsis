#!/usr/bin/env python3
"""
Fix MNOs table to use camelCase
"""

import psycopg2
from config import Config

def fix_mnos_table():
    """Drop and recreate MNOs table with camelCase"""
    config = Config()
    
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Drop existing table
    cursor.execute("DROP TABLE IF EXISTS balances_with_mnos CASCADE;")
    print("🗑️ Dropped old balances_with_mnos table")
    
    # Create new table with camelCase
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS balances_with_mnos (
        "reportingDate" TIMESTAMP,
        "floatBalanceDate" TIMESTAMP,
        "mnoCode" VARCHAR(100),
        "tillNumber" VARCHAR(50),
        currency VARCHAR(10),
        "allowanceProbableLoss" DECIMAL(15,2),
        "botProvision" DECIMAL(15,2),
        "orgFloatAmount" DECIMAL(15,2),
        "usdFloatAmount" DECIMAL(15,2),
        "tzsFloatAmount" DECIMAL(15,2),
        PRIMARY KEY ("tillNumber", "mnoCode")
    );
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    
    print("✅ Created new balances_with_mnos table with camelCase columns")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    fix_mnos_table()