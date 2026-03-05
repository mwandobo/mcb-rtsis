#!/usr/bin/env python3
"""
Create cashInformation table in PostgreSQL
Based on cash-information.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_cash_information_table():
    """Create the cashInformation table in PostgreSQL"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
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
        logger.info("Dropping existing cashInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "cashInformation" CASCADE')
        
        # Create cashInformation table
        logger.info("Creating cashInformation table...")
        create_table_sql = """
        CREATE TABLE "cashInformation" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "branchCode" VARCHAR(50),
            "cashCategory" VARCHAR(100),
            "cashSubCategory" VARCHAR(100),
            "cashSubmissionTime" VARCHAR(50),
            "currency" VARCHAR(10),
            "cashDenomination" VARCHAR(50),
            "quantityOfCoinsNotes" VARCHAR(50),
            "orgAmount" VARCHAR(50),
            "usdAmount" VARCHAR(50),
            "tzsAmount" VARCHAR(50),
            "transactionDate" VARCHAR(12),
            "maturityDate" VARCHAR(12),
            "allowanceProbableLoss" VARCHAR(50),
            "botProvision" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_cashInformation_reporting_date ON "cashInformation"("reportingDate")',
            'CREATE INDEX idx_cashInformation_branch_code ON "cashInformation"("branchCode")',
            'CREATE INDEX idx_cashInformation_cash_category ON "cashInformation"("cashCategory")',
            'CREATE INDEX idx_cashInformation_transaction_date ON "cashInformation"("transactionDate")',
            'CREATE INDEX idx_cashInformation_currency ON "cashInformation"("currency")',
            'CREATE UNIQUE INDEX idx_cashInformation_unique ON "cashInformation"("branchCode", "transactionDate", "cashCategory")',
            'CREATE INDEX idx_cashInformation_created_at ON "cashInformation"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            # Extract index name from SQL
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        # Commit changes
        conn.commit()
        
        # Get table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'cashInformation'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Cash Information table created successfully!")
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
        
        logger.info("cashInformation table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating cashInformation table: {e}")
        raise

if __name__ == "__main__":
    create_cash_information_table()