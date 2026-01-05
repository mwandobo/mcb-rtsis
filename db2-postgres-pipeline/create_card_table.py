#!/usr/bin/env python3
"""
Create Card Information Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_card_table():
    """Create the card information table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing cardInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "cardInformation" CASCADE;')
        
        # Create Card Information table
        logger.info("🏗️ Creating cardInformation table...")
        create_table_sql = """
        CREATE TABLE "cardInformation" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "bankCode" VARCHAR(20),
            "cardNumber" VARCHAR(50),
            "binNumber" VARCHAR(20),
            "customerIdentificationNumber" VARCHAR(50),
            "cardType" VARCHAR(50),
            "cardTypeSubCategory" VARCHAR(50),
            "cardIssueDate" DATE,
            "cardIssuer" VARCHAR(100),
            "cardIssuerCategory" VARCHAR(50),
            "cardIssuerCountry" VARCHAR(50),
            "cardHolderName" VARCHAR(200),
            "cardStatus" VARCHAR(50),
            "cardScheme" VARCHAR(50),
            "acquiringPartner" VARCHAR(100),
            "cardExpireDate" DATE
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_card_number_unique ON "cardInformation"("cardNumber");',
            'CREATE INDEX IF NOT EXISTS idx_card_customer_id ON "cardInformation"("customerIdentificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_card_type ON "cardInformation"("cardType");',
            'CREATE INDEX IF NOT EXISTS idx_card_status ON "cardInformation"("cardStatus");',
            'CREATE INDEX IF NOT EXISTS idx_card_scheme ON "cardInformation"("cardScheme");',
            'CREATE INDEX IF NOT EXISTS idx_card_issue_date ON "cardInformation"("cardIssueDate");',
            'CREATE INDEX IF NOT EXISTS idx_card_expire_date ON "cardInformation"("cardExpireDate");',
            'CREATE INDEX IF NOT EXISTS idx_card_bank_code ON "cardInformation"("bankCode");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Card table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'cardInformation' 
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
        logger.error(f"❌ Failed to create card table: {e}")
        raise

if __name__ == "__main__":
    create_card_table()