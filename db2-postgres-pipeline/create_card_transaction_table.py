#!/usr/bin/env python3
"""
Create Card Transaction Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_card_transaction_table():
    """Create the card transaction table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing cardTransaction table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "cardTransaction" CASCADE;')
        
        # Create Card Transaction table
        logger.info("🏗️ Creating cardTransaction table...")
        create_table_sql = """
        CREATE TABLE "cardTransaction" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "cardNumber" VARCHAR(50),
            "binNumber" VARCHAR(20),
            "transactingBankName" VARCHAR(100),
            "transactionId" VARCHAR(50),
            "transactionDate" DATE,
            "transactionNature" VARCHAR(100),
            "atmCode" VARCHAR(50),
            "posNumber" VARCHAR(50),
            "transactionDescription" VARCHAR(200),
            "beneficiaryName" VARCHAR(200),
            "beneficiaryTradeName" VARCHAR(200),
            "beneficiaryCountry" VARCHAR(50),
            "transactionPlace" VARCHAR(100),
            "qtyItemsPurchased" VARCHAR(20),
            "unitPrice" VARCHAR(20),
            "orgFacilitatorCommissionAmount" VARCHAR(20),
            "usdFacilitatorCommissionAmount" VARCHAR(20),
            "tzsFacilitatorCommissionAmount" VARCHAR(20),
            "currency" VARCHAR(10),
            "orgTransactionAmount" DECIMAL(15,2),
            "usdTransactionAmount" DECIMAL(15,2),
            "tzsTransactionAmount" DECIMAL(15,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_card_transaction_unique ON "cardTransaction"("transactionId");',
            'CREATE INDEX IF NOT EXISTS idx_card_transaction_card_number ON "cardTransaction"("cardNumber");',
            'CREATE INDEX IF NOT EXISTS idx_card_transaction_date ON "cardTransaction"("transactionDate");',
            'CREATE INDEX IF NOT EXISTS idx_card_transaction_nature ON "cardTransaction"("transactionNature");',
            'CREATE INDEX IF NOT EXISTS idx_card_transaction_bank ON "cardTransaction"("transactingBankName");',
            'CREATE INDEX IF NOT EXISTS idx_card_transaction_currency ON "cardTransaction"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_card_transaction_amount ON "cardTransaction"("orgTransactionAmount");',
            'CREATE INDEX IF NOT EXISTS idx_card_transaction_beneficiary ON "cardTransaction"("beneficiaryName");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Card transaction table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'cardTransaction' 
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
        logger.error(f"❌ Failed to create card transaction table: {e}")
        raise

if __name__ == "__main__":
    create_card_transaction_table()