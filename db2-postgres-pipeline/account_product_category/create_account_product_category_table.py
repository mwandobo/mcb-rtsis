#!/usr/bin/env python3
"""
Create accountProductCategory table in PostgreSQL
Based on account-product-category.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_account_product_category_table():
    """Create the accountProductCategory table in PostgreSQL"""
    
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
        logger.info("Dropping existing accountProductCategory table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "accountProductCategory" CASCADE')
        
        # Create accountProductCategory table
        logger.info("Creating accountProductCategory table...")
        create_table_sql = """
        CREATE TABLE "accountProductCategory" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "accountProductCode" VARCHAR(50),
            "accountProductName" VARCHAR(255),
            "accountProductDescription" VARCHAR(255),
            "accountProductType" VARCHAR(50),
            "accountProductSubType" VARCHAR(50),
            currency VARCHAR(10),
            "accountProductCreationDate" VARCHAR(12),
            "accountProductClosureDate" VARCHAR(12),
            "accountProductStatus" VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_apc_code ON "accountProductCategory"("accountProductCode")',
            'CREATE INDEX idx_apc_type ON "accountProductCategory"("accountProductType")',
            'CREATE INDEX idx_apc_status ON "accountProductCategory"("accountProductStatus")',
            'CREATE INDEX idx_apc_currency ON "accountProductCategory"(currency)',
            'CREATE INDEX idx_apc_created ON "accountProductCategory"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        # Get table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'accountProductCategory'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("accountProductCategory table created successfully!")
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
        
        logger.info("accountProductCategory table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating accountProductCategory table: {e}")
        raise

if __name__ == "__main__":
    create_account_product_category_table()