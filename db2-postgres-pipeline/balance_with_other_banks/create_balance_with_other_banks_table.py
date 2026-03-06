#!/usr/bin/env python3
"""
Create balanceWithOtherBanks table in PostgreSQL
Based on balance-with-other-bank-v1.sql structure
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_balance_with_other_banks_table():
    """Create the balanceWithOtherBanks table in PostgreSQL"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        logger.info("Dropping existing balanceWithOtherBanks table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "balanceWithOtherBanks" CASCADE')
        
        logger.info("Creating balanceWithOtherBanks table...")
        create_table_sql = """
        CREATE TABLE "balanceWithOtherBanks" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "accountName" VARCHAR(255),
            "bankCode" VARCHAR(10),
            country VARCHAR(100),
            "relationshipType" VARCHAR(50),
            "accountType" VARCHAR(50),
            "subAccountType" VARCHAR(50),
            currency VARCHAR(10),
            "orgAmount" DECIMAL(18, 2),
            "usdAmount" DECIMAL(18, 2),
            "tzsAmount" DECIMAL(18, 2),
            "transactionDate" VARCHAR(50),
            "pastDueDays" INTEGER,
            "allowanceProbableLoss" INTEGER,
            "botProvision" INTEGER,
            "assetsClassificationCategory" VARCHAR(50),
            "contractDate" VARCHAR(50),
            "maturityDate" VARCHAR(50),
            "externalRatingCorrespondentBank" VARCHAR(50),
            "gradesUnratedBanks" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_balanceWithOtherBanks_reporting_date ON "balanceWithOtherBanks"("reportingDate")',
            'CREATE INDEX idx_balanceWithOtherBanks_account_number ON "balanceWithOtherBanks"("accountNumber")',
            'CREATE INDEX idx_balanceWithOtherBanks_bank_code ON "balanceWithOtherBanks"("bankCode")',
            'CREATE INDEX idx_balanceWithOtherBanks_currency ON "balanceWithOtherBanks"(currency)',
            'CREATE INDEX idx_balanceWithOtherBanks_account_type ON "balanceWithOtherBanks"("accountType")',
            'CREATE INDEX idx_balanceWithOtherBanks_created_at ON "balanceWithOtherBanks"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        conn.commit()
        
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithOtherBanks'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Balance with Other Banks table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<40} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<40} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("balanceWithOtherBanks table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating balanceWithOtherBanks table: {e}")
        raise

if __name__ == "__main__":
    create_balance_with_other_banks_table()
