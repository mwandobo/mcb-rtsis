#!/usr/bin/env python3
"""
Create shareCapital table in PostgreSQL
Based on share-capital.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_share_capital_table():
    """Create the shareCapital table in PostgreSQL"""
    
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
        logger.info("Dropping existing shareCapital table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "shareCapital" CASCADE')
        
        # Create shareCapital table
        logger.info("Creating shareCapital table...")
        create_table_sql = """
        CREATE TABLE "shareCapital" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "capitalCategory" VARCHAR(100),
            "capitalSubCategory" VARCHAR(100),
            "transactionDate" VARCHAR(50),
            "transactionType" VARCHAR(50),
            "shareholderNames" VARCHAR(255),
            "clientType" VARCHAR(50),
            "shareholderCountry" VARCHAR(100),
            "numberOfShares" VARCHAR(50),
            "sharePriceBookValue" VARCHAR(50),
            "currency" VARCHAR(10),
            "orgAmount" VARCHAR(50),
            "tzsAmount" VARCHAR(50),
            "sectorSnaClassification" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_shareCapital_reporting_date ON "shareCapital"("reportingDate")',
            'CREATE INDEX idx_shareCapital_capital_category ON "shareCapital"("capitalCategory")',
            'CREATE INDEX idx_shareCapital_transaction_date ON "shareCapital"("transactionDate")',
            'CREATE INDEX idx_shareCapital_shareholder_names ON "shareCapital"("shareholderNames")',
            'CREATE INDEX idx_shareCapital_client_type ON "shareCapital"("clientType")',
            'CREATE INDEX idx_shareCapital_currency ON "shareCapital"("currency")',
            'CREATE UNIQUE INDEX idx_shareCapital_unique ON "shareCapital"("shareholderNames", "transactionDate", "capitalCategory")',
            'CREATE INDEX idx_shareCapital_created_at ON "shareCapital"(created_at)'
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
            WHERE table_name = 'shareCapital'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Share Capital table created successfully!")
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
        
        logger.info("shareCapital table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating shareCapital table: {e}")
        raise

if __name__ == "__main__":
    create_share_capital_table()