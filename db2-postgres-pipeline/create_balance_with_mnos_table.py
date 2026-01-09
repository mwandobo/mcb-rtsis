#!/usr/bin/env python3
"""
Create Balance with MNOs Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_balance_with_mnos_table():
    """Create the balance with MNOs table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing balanceWithMnos table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "balanceWithMnos" CASCADE;')
        
        # Create Balance with MNOs table
        logger.info("🏗️ Creating balanceWithMnos table...")
        create_table_sql = """
        CREATE TABLE "balanceWithMnos" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" DATE,
            "floatBalanceDate" DATE,
            "mnoCode" VARCHAR(100),
            "tillNumber" VARCHAR(50),
            "currency" VARCHAR(10),
            "allowanceProbableLoss" DECIMAL(15,2),
            "botProvision" DECIMAL(15,2),
            "orgFloatAmount" DECIMAL(15,2),
            "usdFloatAmount" DECIMAL(15,2),
            "tzsFloatAmount" DECIMAL(15,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes (unique constraint on mnoCode to keep latest balance per MNO)
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_balance_mnos_unique ON "balanceWithMnos"("mnoCode");',
            'CREATE INDEX IF NOT EXISTS idx_balance_mnos_till ON "balanceWithMnos"("tillNumber");',
            'CREATE INDEX IF NOT EXISTS idx_balance_mnos_currency ON "balanceWithMnos"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_balance_mnos_date ON "balanceWithMnos"("reportingDate");',
            'CREATE INDEX IF NOT EXISTS idx_balance_mnos_float_date ON "balanceWithMnos"("floatBalanceDate");',
            'CREATE INDEX IF NOT EXISTS idx_balance_mnos_amount ON "balanceWithMnos"("orgFloatAmount");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Balance with MNOs table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithMnos' 
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
        logger.error(f"❌ Failed to create balance with MNOs table: {e}")
        raise

if __name__ == "__main__":
    create_balance_with_mnos_table()