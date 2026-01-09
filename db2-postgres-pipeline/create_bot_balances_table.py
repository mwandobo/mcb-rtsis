#!/usr/bin/env python3
"""
Create BOT balances table in PostgreSQL for RTSIS reporting
"""

import psycopg2
from config import Config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_bot_balances_table():
    """Create BOT balances table with proper structure and indexes"""
    
    # Initialize config
    config = Config()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS "balancesBot" (
        "id" SERIAL PRIMARY KEY,
        "reportingDate" TIMESTAMP NOT NULL,
        "accountNumber" VARCHAR(50) NOT NULL,
        "accountName" VARCHAR(255) NOT NULL,
        "accountType" VARCHAR(100) NOT NULL,
        "subAccountType" VARCHAR(100),
        "currency" VARCHAR(10) NOT NULL,
        "orgAmount" DECIMAL(18,2) NOT NULL,
        "usdAmount" DECIMAL(18,2),
        "tzsAmount" DECIMAL(18,2) NOT NULL,
        "transactionDate" DATE NOT NULL,
        "maturityDate" TIMESTAMP NOT NULL,
        "allowanceProbableLoss" DECIMAL(18,2) DEFAULT 0.0,
        "botProvision" DECIMAL(18,2) DEFAULT 0.0,
        "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create indexes for better performance
    create_indexes_sql = [
        'CREATE INDEX IF NOT EXISTS "idx_balances_bot_account_number" ON "balancesBot" ("accountNumber");',
        'CREATE INDEX IF NOT EXISTS "idx_balances_bot_currency" ON "balancesBot" ("currency");',
        'CREATE INDEX IF NOT EXISTS "idx_balances_bot_reporting_date" ON "balancesBot" ("reportingDate");',
        'CREATE INDEX IF NOT EXISTS "idx_balances_bot_transaction_date" ON "balancesBot" ("transactionDate");',
        'CREATE INDEX IF NOT EXISTS "idx_balances_bot_account_type" ON "balancesBot" ("accountType");'
    ]
    
    # Create trigger for updated_at
    create_trigger_sql = """
    CREATE OR REPLACE FUNCTION update_balances_bot_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW."updated_at" = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    
    DROP TRIGGER IF EXISTS "trigger_balances_bot_updated_at" ON "balancesBot";
    CREATE TRIGGER "trigger_balances_bot_updated_at"
        BEFORE UPDATE ON "balancesBot"
        FOR EACH ROW
        EXECUTE FUNCTION update_balances_bot_updated_at();
    """
    
    try:
        # Connect to PostgreSQL
        pg_config = {
            'host': config.database.pg_host,
            'port': config.database.pg_port,
            'database': config.database.pg_database,
            'user': config.database.pg_user,
            'password': config.database.pg_password
        }
        conn = psycopg2.connect(**pg_config)
        cursor = conn.cursor()
        
        logger.info("🏦 Creating BOT balances table...")
        
        # Create table
        cursor.execute(create_table_sql)
        logger.info("✅ BOT balances table created successfully")
        
        # Create indexes
        for index_sql in create_indexes_sql:
            cursor.execute(index_sql)
        logger.info("✅ BOT balances table indexes created successfully")
        
        # Create trigger
        cursor.execute(create_trigger_sql)
        logger.info("✅ BOT balances table trigger created successfully")
        
        # Commit changes
        conn.commit()
        
        # Verify table creation
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'balancesBot'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info(f"✅ BOT balances table created with {len(columns)} columns:")
        for col_name, data_type, nullable in columns:
            logger.info(f"   - {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
        
        cursor.close()
        conn.close()
        
        logger.info("🎉 BOT balances table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error creating BOT balances table: {str(e)}")
        raise

if __name__ == "__main__":
    create_bot_balances_table()