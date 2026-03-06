#!/usr/bin/env python3
"""
Create loanTransactions table in PostgreSQL
Based on loan-transaction.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_loan_transactions_table():
    """Create the loanTransactions table in PostgreSQL"""
    
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
        logger.info("Dropping existing loanTransactions table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "loanTransactions" CASCADE')
        
        # Create loanTransactions table
        logger.info("Creating loanTransactions table...")
        create_table_sql = """
        CREATE TABLE "loanTransactions" (
            id SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "loanNumber" VARCHAR(50),
            "transactionDate" TIMESTAMP,
            "loanTransactionType" VARCHAR(100),
            "loanTransactionSubType" VARCHAR(100),
            "currency" VARCHAR(10),
            "orgTransactionAmount" VARCHAR(50),
            "usdTransactionAmount" VARCHAR(50),
            "tzsTransactionAmount" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_loantransactions_reporting_date ON "loanTransactions"("reportingDate")',
            'CREATE INDEX idx_loantransactions_loan_number ON "loanTransactions"("loanNumber")',
            'CREATE INDEX idx_loantransactions_transaction_date ON "loanTransactions"("transactionDate")',
            'CREATE INDEX idx_loantransactions_transaction_type ON "loanTransactions"("loanTransactionType")',
            'CREATE INDEX idx_loantransactions_currency ON "loanTransactions"("currency")',
            'CREATE INDEX idx_loantransactions_created_at ON "loanTransactions"(created_at)'
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
            WHERE table_name = 'loanTransactions'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Loan Transactions table created successfully!")
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
        
        logger.info("loanTransactions table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating loanTransactions table: {e}")
        raise

if __name__ == "__main__":
    create_loan_transactions_table()
