#!/usr/bin/env python3
"""
Create atmTransactions table in PostgreSQL
Based on atm-transaction-v1.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_atm_transactions_table():
    """Create the atmTransactions table in PostgreSQL"""
    
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
        logger.info("Dropping existing atmTransactions table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "atmTransactions" CASCADE')
        
        # Create atmTransactions table
        logger.info("Creating atmTransactions table...")
        create_table_sql = """
        CREATE TABLE "atmTransactions" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "atmCode" VARCHAR(50),
            "transactionDate" VARCHAR(12),
            "transactionId" VARCHAR(255),
            "transactionType" VARCHAR(100),
            "currency" VARCHAR(10),
            "orgTransactionAmount" DECIMAL(18,2),
            "tzsTransactionAmount" DECIMAL(18,2),
            "atmChannel" VARCHAR(50),
            "valueAddedTaxAmount" DECIMAL(15,2),
            "exciseDutyAmount" DECIMAL(18,2),
            "electronicLevyAmount" DECIMAL(18,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_atmTransactions_atm_code ON "atmTransactions"("atmCode")',
            'CREATE INDEX idx_atmTransactions_transaction_date ON "atmTransactions"("transactionDate")',
            'CREATE INDEX idx_atmTransactions_transaction_id ON "atmTransactions"("transactionId")',
            'CREATE INDEX idx_atmTransactions_transaction_type ON "atmTransactions"("transactionType")',
            'CREATE INDEX idx_atmTransactions_reporting_date ON "atmTransactions"("reportingDate")',
            'CREATE INDEX idx_atmTransactions_created_at ON "atmTransactions"(created_at)'
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
            WHERE table_name = 'atmTransactions'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("ATM Transactions table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<25} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<25} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("atmTransactions table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating atmTransactions table: {e}")
        raise

if __name__ == "__main__":
    create_atm_transactions_table()