#!/usr/bin/env python3
"""
Recreate all pipeline tables without primary key constraints
"""

import psycopg2
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def recreate_tables_no_pk():
    """Drop and recreate all pipeline tables without primary keys"""
    config = Config()
    
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Tables to recreate (pipeline tables only)
    pipeline_tables = [
        'cash_information',
        'asset_owned', 
        'balances_bot',
        'balances_with_mnos',
        'balance_with_other_bank',
        'other_assets'
    ]
    
    logger.info("🗑️ Dropping existing tables...")
    
    # Drop existing tables
    for table in pipeline_tables:
        try:
            cursor.execute(f'DROP TABLE IF EXISTS {table}')
            logger.info(f"   Dropped: {table}")
        except Exception as e:
            logger.warning(f"   Failed to drop {table}: {e}")
    
    conn.commit()
    
    logger.info("✅ Creating tables without primary keys...")
    
    # Create tables without primary keys
    table_definitions = {
        'cash_information': """
            CREATE TABLE cash_information (
                "reportingDate" TIMESTAMP,
                "branchCode" INTEGER,
                "cashCategory" VARCHAR(50),
                "cashSubCategory" VARCHAR(50),
                "cashSubmissionTime" VARCHAR(50),
                currency VARCHAR(10),
                "cashDenomination" VARCHAR(50),
                "quantityOfCoinsNotes" INTEGER,
                "orgAmount" DECIMAL(15,2),
                "usdAmount" DECIMAL(15,2),
                "tzsAmount" DECIMAL(15,2),
                "transactionDate" DATE,
                "maturityDate" DATE,
                "allowanceProbableLoss" DECIMAL(15,2),
                "botProvision" DECIMAL(15,2)
            )
        """,
        
        'asset_owned': """
            CREATE TABLE asset_owned (
                "reportingDate" TIMESTAMP,
                "acquisitionDate" DATE,
                currency VARCHAR(10),
                "assetCategory" VARCHAR(50),
                "assetType" VARCHAR(100),
                "orgCostValue" DECIMAL(15,2),
                "usdCostValue" DECIMAL(15,2),
                "tzsCostValue" DECIMAL(15,2),
                "allowanceProbableLoss" DECIMAL(15,2),
                "botProvision" DECIMAL(15,2)
            )
        """,
        
        'balances_bot': """
            CREATE TABLE balances_bot (
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
                "botProvision" DECIMAL(15,2)
            )
        """,
        
        'balances_with_mnos': """
            CREATE TABLE balances_with_mnos (
                "reportingDate" TIMESTAMP,
                "floatBalanceDate" TIMESTAMP,
                "mnoCode" VARCHAR(100),
                "tillNumber" VARCHAR(50),
                currency VARCHAR(10),
                "allowanceProbableLoss" DECIMAL(15,2),
                "botProvision" DECIMAL(15,2),
                "orgFloatAmount" DECIMAL(15,2),
                "usdFloatAmount" DECIMAL(15,2),
                "tzsFloatAmount" DECIMAL(15,2)
            )
        """,
        
        'balance_with_other_bank': """
            CREATE TABLE balance_with_other_bank (
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
                "gradesUnratedBanks" VARCHAR(50)
            )
        """,
        
        'other_assets': """
            CREATE TABLE other_assets (
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
        """
    }
    
    # Create each table
    for table_name, definition in table_definitions.items():
        try:
            cursor.execute(definition)
            logger.info(f"   Created: {table_name}")
        except Exception as e:
            logger.error(f"   Failed to create {table_name}: {e}")
    
    conn.commit()
    
    # Create indexes for performance (but no unique constraints)
    logger.info("📊 Creating performance indexes...")
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_cash_info_date ON cash_information(\"transactionDate\")",
        "CREATE INDEX IF NOT EXISTS idx_cash_info_branch ON cash_information(\"branchCode\")",
        "CREATE INDEX IF NOT EXISTS idx_asset_owned_type ON asset_owned(\"assetType\")",
        "CREATE INDEX IF NOT EXISTS idx_balances_bot_account ON balances_bot(\"accountNumber\")",
        "CREATE INDEX IF NOT EXISTS idx_balances_mnos_till ON balances_with_mnos(\"tillNumber\")",
        "CREATE INDEX IF NOT EXISTS idx_other_banks_account ON balance_with_other_bank(\"accountNumber\")",
        "CREATE INDEX IF NOT EXISTS idx_other_assets_type ON other_assets(\"assetType\")"
    ]
    
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
            logger.info(f"   Created index")
        except Exception as e:
            logger.warning(f"   Index creation failed: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info("🎉 All tables recreated without primary key constraints!")
    logger.info("   ✅ No more data loss due to duplicate keys")
    logger.info("   ✅ All fetched records will be preserved")
    logger.info("   ✅ Performance indexes added for queries")

if __name__ == "__main__":
    recreate_tables_no_pk()