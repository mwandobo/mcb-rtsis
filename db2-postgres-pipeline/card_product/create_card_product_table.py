#!/usr/bin/env python3
"""
Create cardProduct table in PostgreSQL
Based on card-product.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_card_product_table():
    """Create the cardProduct table in PostgreSQL"""
    
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
        logger.info("Dropping existing cardProduct table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "cardProduct" CASCADE')
        
        # Create cardProduct table
        logger.info("Creating cardProduct table...")
        create_table_sql = """
        CREATE TABLE "cardProduct" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "binNumber" VARCHAR(6),
            "binNumberStartDate" VARCHAR(12),
            currency VARCHAR(3),
            "cardType" VARCHAR(50),
            "cardTypeSubCategory" VARCHAR(50),
            "cardSchemeName" VARCHAR(50),
            "cardIssuerCategory" VARCHAR(50),
            "cardIssuer" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_card_product_bin ON "cardProduct"("binNumber")',
            'CREATE INDEX idx_card_product_type ON "cardProduct"("cardType")',
            'CREATE INDEX idx_card_product_scheme ON "cardProduct"("cardSchemeName")',
            'CREATE INDEX idx_card_product_created ON "cardProduct"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        # Get table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'cardProduct'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("cardProduct table created successfully!")
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
        
        logger.info("cardProduct table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating cardProduct table: {e}")
        raise

if __name__ == "__main__":
    create_card_product_table()