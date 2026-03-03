#!/usr/bin/env python3
"""
Create cardTransactions table in PostgreSQL
Based on card-transaction-v1.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_card_transactions_table():
    """Create the cardTransactions table in PostgreSQL"""
    
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
        logger.info("Dropping existing cardTransactions table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "cardTransactions" CASCADE')
        
        # Create cardTransactions table
        logger.info("Creating cardTransactions table...")
        create_table_sql = """
        CREATE TABLE "cardTransactions" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "cardNumber" VARCHAR(50),
            "binNumber" VARCHAR(20),
            "transactingBankName" VARCHAR(100),
            "transactionId" VARCHAR(255),
            "transactionDate" VARCHAR(12),
            "transactionNature" VARCHAR(100),
            "atmCode" VARCHAR(50),
            "posNumber" VARCHAR(50),
            "transactionDescription" VARCHAR(255),
            "beneficiaryName" VARCHAR(100),
            "beneficiaryTradeName" VARCHAR(100),
            "beneficaryCountry" VARCHAR(100),
            "transactionPlace" VARCHAR(100),
            "qtyItemsPurchased" VARCHAR(50),
            "unitPrice" VARCHAR(50),
            "orgFacilitatorCommissionAmount" DECIMAL(18,2),
            "usdFacilitatorCommissionAmount" DECIMAL(18,2),
            "tzsFacilitatorCommissionAmount" DECIMAL(18,2),
            "currency" VARCHAR(10),
            "orgTransactionAmount" DECIMAL(18,2),
            "usdTransactionAmount" DECIMAL(18,2),
            "tzsTransactionAmount" DECIMAL(18,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_cardTransactions_card_number ON "cardTransactions"("cardNumber")',
            'CREATE INDEX idx_cardTransactions_transaction_date ON "cardTransactions"("transactionDate")',
            'CREATE INDEX idx_cardTransactions_transaction_id ON "cardTransactions"("transactionId")',
            'CREATE INDEX idx_cardTransactions_bin_number ON "cardTransactions"("binNumber")',
            'CREATE INDEX idx_cardTransactions_reporting_date ON "cardTransactions"("reportingDate")',
            'CREATE INDEX idx_cardTransactions_currency ON "cardTransactions"("currency")',
            'CREATE INDEX idx_cardTransactions_created_at ON "cardTransactions"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            logger.info(f"Created index: {index_sql.split('ON')[0].split('CREATE INDEX')[1].strip()}")
        
        # Commit changes
        conn.commit()
        
        # Get table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'cardTransactions'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Card Transactions table created successfully!")
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
        
        logger.info("cardTransactions table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating cardTransactions table: {e}")
        raise

if __name__ == "__main__":
    create_card_transactions_table()