#!/usr/bin/env python3
"""
Create interbankLoansPayable table in PostgreSQL
Based on interbank_loans_payable.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_interbank_loans_payable_table():
    """Create the interbankLoansPayable table in PostgreSQL"""
    
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
        logger.info("Dropping existing interbankLoansPayable table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "interbankLoansPayable" CASCADE')
        
        # Create interbankLoansPayable table
        logger.info("Creating interbankLoansPayable table...")
        create_table_sql = """
        CREATE TABLE "interbankLoansPayable" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "lenderName" VARCHAR(255),
            "accountNumber" VARCHAR(50),
            "lenderCountry" VARCHAR(100),
            "borrowingType" VARCHAR(50),
            "transactionDate" VARCHAR(12),
            "disbursementDate" VARCHAR(12),
            "maturityDate" VARCHAR(12),
            "currency" VARCHAR(10),
            "orgAmountOpening" VARCHAR(50),
            "usdAmountOpening" VARCHAR(50),
            "tzsAmountOpening" VARCHAR(50),
            "orgAmountRepayment" VARCHAR(50),
            "usdAmountRepayment" VARCHAR(50),
            "tzsAmountRepayment" VARCHAR(50),
            "orgAmountClosing" VARCHAR(50),
            "usdAmountClosing" VARCHAR(50),
            "tzsAmountClosing" VARCHAR(50),
            "tenureDays" VARCHAR(10),
            "annualInterestRate" VARCHAR(20),
            "interestRateType" VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_interbankLoansPayable_reporting_date ON "interbankLoansPayable"("reportingDate")',
            'CREATE UNIQUE INDEX idx_interbankLoansPayable_account_number ON "interbankLoansPayable"("accountNumber")',
            'CREATE INDEX idx_interbankLoansPayable_lender_name ON "interbankLoansPayable"("lenderName")',
            'CREATE INDEX idx_interbankLoansPayable_lender_country ON "interbankLoansPayable"("lenderCountry")',
            'CREATE INDEX idx_interbankLoansPayable_borrowing_type ON "interbankLoansPayable"("borrowingType")',
            'CREATE INDEX idx_interbankLoansPayable_transaction_date ON "interbankLoansPayable"("transactionDate")',
            'CREATE INDEX idx_interbankLoansPayable_disbursement_date ON "interbankLoansPayable"("disbursementDate")',
            'CREATE INDEX idx_interbankLoansPayable_maturity_date ON "interbankLoansPayable"("maturityDate")',
            'CREATE INDEX idx_interbankLoansPayable_currency ON "interbankLoansPayable"("currency")',
            'CREATE INDEX idx_interbankLoansPayable_interest_rate_type ON "interbankLoansPayable"("interestRateType")',
            'CREATE INDEX idx_interbankLoansPayable_created_at ON "interbankLoansPayable"(created_at)'
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
            WHERE table_name = 'interbankLoansPayable'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Interbank Loans Payable table created successfully!")
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
        
        logger.info("interbankLoansPayable table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating interbankLoansPayable table: {e}")
        raise

if __name__ == "__main__":
    create_interbank_loans_payable_table()