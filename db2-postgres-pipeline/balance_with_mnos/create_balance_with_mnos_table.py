#!/usr/bin/env python3
"""
Create balanceWithMnos table in PostgreSQL
Based on balances-with-mnos.sql structure
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_balance_with_mnos_table():
    """Create the balanceWithMnos table in PostgreSQL"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        logger.info("Dropping existing balanceWithMnos table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "balanceWithMnos" CASCADE')
        
        logger.info("Creating balanceWithMnos table...")
        create_table_sql = """
        CREATE TABLE "balanceWithMnos" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "floatBalanceDate" VARCHAR(12),
            "mnoCode" VARCHAR(50),
            "tillNumber" VARCHAR(50),
            currency VARCHAR(10),
            "allowanceProbableLoss" INTEGER,
            "botProvision" INTEGER,
            "orgFloatAmount" DECIMAL(18, 2),
            "usdFloatAmount" DECIMAL(18, 2),
            "tzsFloatAmount" DECIMAL(18, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_balanceWithMnos_reporting_date ON "balanceWithMnos"("reportingDate")',
            'CREATE INDEX idx_balanceWithMnos_mno_code ON "balanceWithMnos"("mnoCode")',
            'CREATE INDEX idx_balanceWithMnos_currency ON "balanceWithMnos"(currency)',
            'CREATE INDEX idx_balanceWithMnos_created_at ON "balanceWithMnos"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        conn.commit()
        
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'balanceWithMnos'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Balance with MNOs table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<35} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<35} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("Balance with MNOs table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating balanceWithMnos table: {e}")
        raise

if __name__ == "__main__":
    create_balance_with_mnos_table()
