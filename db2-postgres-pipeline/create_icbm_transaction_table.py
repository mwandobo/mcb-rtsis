#!/usr/bin/env python3
"""
Create ICBM Transaction Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_icbm_transaction_table():
    """Create the ICBM transaction table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing icbmTransaction table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "icbmTransaction" CASCADE;')
        
        # Create ICBM Transaction table
        logger.info("🏗️ Creating icbmTransaction table...")
        create_table_sql = """
        CREATE TABLE "icbmTransaction" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "transactionDate" DATE,
            "lenderName" VARCHAR(200),
            "borrowerName" VARCHAR(200),
            "transactionType" VARCHAR(50),
            "tzsAmount" DECIMAL(18,2),
            "tenure" VARCHAR(50),
            "interestRate" VARCHAR(50)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_icbm_transaction_date ON "icbmTransaction"("transactionDate");',
            'CREATE INDEX IF NOT EXISTS idx_icbm_transaction_type ON "icbmTransaction"("transactionType");',
            'CREATE INDEX IF NOT EXISTS idx_icbm_transaction_amount ON "icbmTransaction"("tzsAmount");',
            'CREATE INDEX IF NOT EXISTS idx_icbm_transaction_lender ON "icbmTransaction"("lenderName");',
            'CREATE INDEX IF NOT EXISTS idx_icbm_transaction_borrower ON "icbmTransaction"("borrowerName");',
            'CREATE INDEX IF NOT EXISTS idx_icbm_transaction_reporting_date ON "icbmTransaction"("reportingDate");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ ICBM transaction table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'icbmTransaction' 
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
        logger.error(f"❌ Failed to create ICBM transaction table: {e}")
        raise

if __name__ == "__main__":
    create_icbm_transaction_table()