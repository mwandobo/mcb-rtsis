#!/usr/bin/env python3
"""
Create balancesBot table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_balances_bot_table():
    """Create the balancesBot table"""
    config = Config()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS "balancesBot" (
        id SERIAL,
        "reportingDate" VARCHAR(50),
        "accountNumber" VARCHAR(100),
        "accountName" VARCHAR(255),
        "accountType" VARCHAR(100),
        "subAccountType" VARCHAR(100),
        currency VARCHAR(10),
        "orgAmount" NUMERIC(18, 2),
        "usdAmount" NUMERIC(18, 2),
        "tzsAmount" NUMERIC(18, 2),
        "transactionDate" VARCHAR(50),
        "maturityDate" VARCHAR(50),
        "allowanceProbableLoss" INTEGER,
        "botProvision" INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_balances_bot_account_number ON "balancesBot"("accountNumber");
    CREATE INDEX IF NOT EXISTS idx_balances_bot_reporting_date ON "balancesBot"("reportingDate");
    CREATE INDEX IF NOT EXISTS idx_balances_bot_transaction_date ON "balancesBot"("transactionDate");
    """
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        
        logger.info("Table 'balancesBot' created successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise

if __name__ == "__main__":
    create_balances_bot_table()
