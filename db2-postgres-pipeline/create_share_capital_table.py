#!/usr/bin/env python3
"""
Create shareCapital table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_share_capital_table():
    """Create the shareCapital table"""
    config = Config()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    create_table_sql = """
    DROP TABLE IF EXISTS "shareCapital";
    
    CREATE TABLE "shareCapital" (
        id SERIAL,
        "reportingDate" VARCHAR(50),
        "capitalCategory" VARCHAR(255),
        "capitalSubCategory" VARCHAR(255),
        "transactionDate" VARCHAR(50),
        "transactionType" VARCHAR(255),
        "shareholderNames" VARCHAR(500),
        "clientType" VARCHAR(255),
        "shareholderCountry" VARCHAR(255),
        "numberOfShares" NUMERIC(18, 2),
        "sharePriceBookValue" NUMERIC(18, 2),
        currency VARCHAR(10),
        "orgAmount" NUMERIC(18, 2),
        "tzsAmount" NUMERIC(18, 2),
        "sectorSnaClassification" VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_share_capital_reporting_date ON "shareCapital"("reportingDate");
    CREATE INDEX idx_share_capital_transaction_date ON "shareCapital"("transactionDate");
    CREATE INDEX idx_share_capital_capital_category ON "shareCapital"("capitalCategory");
    CREATE INDEX idx_share_capital_shareholder_names ON "shareCapital"("shareholderNames");
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
        
        logger.info("Table 'shareCapital' created successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise

if __name__ == "__main__":
    create_share_capital_table()