#!/usr/bin/env python3
"""
Create deposits table in PostgreSQL
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_deposits_table():
    """Create deposits table with all required columns"""
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
        
        # Drop existing table if exists
        logger.info("Dropping existing deposits table if exists...")
        cursor.execute('DROP TABLE IF EXISTS "deposits" CASCADE')
        conn.commit()
        
        # Create table
        logger.info("Creating deposits table...")
        create_table_sql = """
        CREATE TABLE "deposits" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50) NOT NULL,
            "clientIdentificationNumber" VARCHAR(100) NOT NULL,
            "accountNumber" VARCHAR(100) NOT NULL,
            "accountName" VARCHAR(255),
            "customerCategory" VARCHAR(100),
            "customerCountry" VARCHAR(100),
            "branchCode" VARCHAR(50),
            "clientType" VARCHAR(50),
            "relationshipType" VARCHAR(100),
            "district" VARCHAR(100),
            "region" VARCHAR(100),
            "accountProductName" VARCHAR(255),
            "accountType" VARCHAR(50),
            "accountSubType" VARCHAR(50),
            "depositCategory" VARCHAR(100),
            "depositAccountStatus" VARCHAR(50),
            "transactionUniqueRef" VARCHAR(255) NOT NULL UNIQUE,
            "timeStamp" VARCHAR(50),
            "serviceChannel" VARCHAR(100),
            "currency" VARCHAR(10),
            "transactionType" VARCHAR(50),
            "orgTransactionAmount" VARCHAR(50),
            "usdTransactionAmount" VARCHAR(50),
            "tzsTransactionAmount" VARCHAR(50),
            "transactionPurposes" TEXT,
            "sectorSnaClassification" VARCHAR(100),
            "lienNumber" VARCHAR(100),
            "orgAmountLien" VARCHAR(50),
            "usdAmountLien" VARCHAR(50),
            "tzsAmountLien" VARCHAR(50),
            "contractDate" VARCHAR(50),
            "maturityDate" VARCHAR(50),
            "annualInterestRate" VARCHAR(50),
            "interestRateType" VARCHAR(50),
            "orgInterestAmount" VARCHAR(50),
            "usdInterestAmount" VARCHAR(50),
            "tzsInterestAmount" VARCHAR(50),
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Create indexes
        logger.info("Creating indexes...")
        indexes = [
            'CREATE INDEX idx_deposits_reporting_date ON "deposits" ("reportingDate")',
            'CREATE INDEX idx_deposits_client_id ON "deposits" ("clientIdentificationNumber")',
            'CREATE INDEX idx_deposits_account_number ON "deposits" ("accountNumber")',
            'CREATE INDEX idx_deposits_region ON "deposits" ("region")',
            'CREATE INDEX idx_deposits_district ON "deposits" ("district")',
            'CREATE INDEX idx_deposits_currency ON "deposits" ("currency")',
            'CREATE INDEX idx_deposits_transaction_type ON "deposits" ("transactionType")',
            'CREATE INDEX idx_deposits_created_at ON "deposits" ("createdAt")',
            'CREATE UNIQUE INDEX idx_deposits_transaction_unique_ref ON "deposits" ("transactionUniqueRef")'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            logger.info(f"Created index: {index_sql.split('INDEX')[1].split('ON')[0].strip()}")
        
        conn.commit()
        
        logger.info("Deposits table created successfully!")
        logger.info("Total columns: 37")
        
        cursor.close()
        conn.close()
        
        logger.info("deposits table setup completed!")
        
    except Exception as e:
        logger.error(f"Error creating deposits table: {e}")
        raise


if __name__ == "__main__":
    create_deposits_table()
