#!/usr/bin/env python3
"""
Create ATM Transaction Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_atm_transaction_table():
    """Create the ATM transaction table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing atmTransaction table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "atmTransaction" CASCADE;')
        
        # Create ATM Transaction table
        logger.info("🏗️ Creating atmTransaction table...")
        create_table_sql = """
        CREATE TABLE "atmTransaction" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "atmCode" VARCHAR(50),
            "transactionDate" DATE,
            "transactionId" VARCHAR(100),
            "transactionNature" VARCHAR(50),
            "currency" VARCHAR(10),
            "orgTransactionAmount" DECIMAL(15,2),
            "tzsTransactionAmount" DECIMAL(15,2),
            "atmChannel" VARCHAR(50),
            "valueAddedTaxAmount" DECIMAL(15,2),
            "exciseDutyAmount" DECIMAL(15,2),
            "electronicLevyAmount" DECIMAL(15,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_atm_transaction_unique ON "atmTransaction"("transactionId");',
            'CREATE INDEX IF NOT EXISTS idx_atm_transaction_code ON "atmTransaction"("atmCode");',
            'CREATE INDEX IF NOT EXISTS idx_atm_transaction_date ON "atmTransaction"("transactionDate");',
            'CREATE INDEX IF NOT EXISTS idx_atm_transaction_nature ON "atmTransaction"("transactionNature");',
            'CREATE INDEX IF NOT EXISTS idx_atm_transaction_currency ON "atmTransaction"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_atm_transaction_amount ON "atmTransaction"("orgTransactionAmount");',
            'CREATE INDEX IF NOT EXISTS idx_atm_transaction_channel ON "atmTransaction"("atmChannel");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ ATM transaction table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'atmTransaction' 
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
        logger.error(f"❌ Failed to create ATM transaction table: {e}")
        raise

if __name__ == "__main__":
    create_atm_transaction_table()