#!/usr/bin/env python3
"""
Create Banker Cheques and Drafts Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_banker_cheques_drafts_table():
    """Create the banker cheques and drafts table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing bankerChequesDrafts table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "bankerChequesDrafts" CASCADE;')
        
        # Create Banker Cheques and Drafts table
        logger.info("🏗️ Creating bankerChequesDrafts table...")
        create_table_sql = """
        CREATE TABLE "bankerChequesDrafts" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "customerIdentificationNumber" VARCHAR(50),
            "customerName" VARCHAR(200),
            "beneficiaryName" VARCHAR(200),
            "checkNumber" VARCHAR(50),
            "transactionDate" DATE,
            "valueDate" DATE,
            "maturityDate" DATE,
            "currency" VARCHAR(10),
            "orgAmount" DECIMAL(18,2),
            "usdAmount" DECIMAL(18,2),
            "tzsAmount" DECIMAL(18,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_customer ON "bankerChequesDrafts"("customerIdentificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_customer_name ON "bankerChequesDrafts"("customerName");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_beneficiary ON "bankerChequesDrafts"("beneficiaryName");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_check_number ON "bankerChequesDrafts"("checkNumber");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_transaction_date ON "bankerChequesDrafts"("transactionDate");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_value_date ON "bankerChequesDrafts"("valueDate");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_currency ON "bankerChequesDrafts"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_amount ON "bankerChequesDrafts"("orgAmount");',
            'CREATE INDEX IF NOT EXISTS idx_banker_cheques_reporting_date ON "bankerChequesDrafts"("reportingDate");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Banker cheques and drafts table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'bankerChequesDrafts' 
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
        logger.error(f"❌ Failed to create banker cheques and drafts table: {e}")
        raise

if __name__ == "__main__":
    create_banker_cheques_drafts_table()