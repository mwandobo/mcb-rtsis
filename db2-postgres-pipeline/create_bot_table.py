#!/usr/bin/env python3
"""
Create BOT balances table
"""

import psycopg2
from config import Config

def create_bot_table():
    """Create BOT balances table"""
    config = Config()
    
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Create BOT balances table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS balances_bot (
        "reportingDate" TIMESTAMP,
        "accountNumber" VARCHAR(50),
        "accountName" VARCHAR(100),
        "accountType" VARCHAR(50),
        "subAccountType" VARCHAR(50),
        currency VARCHAR(10),
        "orgAmount" DECIMAL(15,2),
        "usdAmount" DECIMAL(15,2),
        "tzsAmount" DECIMAL(15,2),
        "transactionDate" DATE,
        "maturityDate" TIMESTAMP,
        "allowanceProbableLoss" DECIMAL(15,2),
        "botProvision" DECIMAL(15,2),
        PRIMARY KEY ("accountNumber", "transactionDate")
    );
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    
    print("✅ BOT balances table created successfully")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_bot_table()