#!/usr/bin/env python3
"""
Create Balance with Other Banks Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_balance_with_other_banks_table():
    """Create the balance with other banks table in PostgreSQL"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        # Drop table if exists
        logger.info("🗑️ Dropping existing balanceWithOtherBank table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "balanceWithOtherBank" CASCADE;')
        
        # Create Balance with Other Banks table
        logger.info("🏗️ Creating balanceWithOtherBank table...")
        create_table_sql = """
        CREATE TABLE "balanceWithOtherBank" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "accountNumber" VARCHAR(50),
            "accountName" VARCHAR(200),
            "bankCode" VARCHAR(50),
            "country" VARCHAR(50),
            "relationshipType" VARCHAR(100),
            "accountType" VARCHAR(50),
            "subAccountType" VARCHAR(50),
            "currency" VARCHAR(10),
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
            "externalRatingCorrespondentBank" VARCHAR(200),
            "gradesUnratedBanks" VARCHAR(100)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_balance_other_banks_unique ON "balanceWithOtherBank"("accountNumber", "transactionDate");',
            'CREATE INDEX IF NOT EXISTS idx_balance_other_banks_account ON "balanceWithOtherBank"("accountNumber");',
            'CREATE INDEX IF NOT EXISTS idx_balance_other_banks_bank_code ON "balanceWithOtherBank"("bankCode");',
            'CREATE INDEX IF NOT EXISTS idx_balance_other_banks_currency ON "balanceWithOtherBank"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_balance_other_banks_date ON "balanceWithOtherBank"("transactionDate");',
            'CREATE INDEX IF NOT EXISTS idx_balance_other_banks_amount ON "balanceWithOtherBank"("orgAmount");',
            'CREATE INDEX IF NOT EXISTS idx_balance_other_banks_country ON "balanceWithOtherBank"("country");',
            'CREATE INDEX IF NOT EXISTS idx_balance_other_banks_relationship ON "balanceWithOtherBank"("relationshipType");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Balance with other banks table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithOtherBank' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for col_name, data_type, max_length in columns:
            length_info = f"({max_length})" if max_length else ""
            logger.info(f"  {col_name}: {data_type}{length_info}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to create balance with other banks table: {e}")
        raise

if __name__ == "__main__":
    create_balance_with_other_banks_table()