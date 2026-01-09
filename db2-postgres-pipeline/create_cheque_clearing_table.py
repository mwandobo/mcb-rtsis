#!/usr/bin/env python3
"""
Create Cheque and Other Items for Clearing Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_cheque_clearing_table():
    """Create the cheque and other items for clearing table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing chequeClearing table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "chequeClearing" CASCADE;')
        
        # Create Cheque Clearing table
        logger.info("🏗️ Creating chequeClearing table...")
        create_table_sql = """
        CREATE TABLE "chequeClearing" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "chequeNumber" VARCHAR(50),
            "issuerName" VARCHAR(200),
            "issuerBankerCode" VARCHAR(20),
            "payeeName" VARCHAR(200),
            "payeeAccountNumber" VARCHAR(50),
            "chequeDate" DATE,
            "transactionDate" DATE,
            "settlementDate" DATE,
            "allowanceProbableLoss" DECIMAL(15,2),
            "botProvision" DECIMAL(15,2),
            "currency" VARCHAR(10),
            "orgAmountOpening" DECIMAL(15,2),
            "usdAmountOpening" DECIMAL(15,2),
            "tzsAmountOpening" DECIMAL(15,2),
            "orgAmountPayment" DECIMAL(15,2),
            "usdAmountPayment" DECIMAL(15,2),
            "tzsAmountPayment" DECIMAL(15,2),
            "orgAmountBalance" DECIMAL(15,2),
            "usdAmountBalance" DECIMAL(15,2),
            "tzsAmountBalance" DECIMAL(15,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_cheque_clearing_unique ON "chequeClearing"("chequeNumber");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_issuer ON "chequeClearing"("issuerName");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_payee ON "chequeClearing"("payeeName");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_banker_code ON "chequeClearing"("issuerBankerCode");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_account ON "chequeClearing"("payeeAccountNumber");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_date ON "chequeClearing"("chequeDate");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_transaction_date ON "chequeClearing"("transactionDate");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_settlement_date ON "chequeClearing"("settlementDate");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_currency ON "chequeClearing"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_cheque_clearing_amount ON "chequeClearing"("orgAmountPayment");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Cheque clearing table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'chequeClearing' 
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
        logger.error(f"❌ Failed to create cheque clearing table: {e}")
        raise

if __name__ == "__main__":
    create_cheque_clearing_table()