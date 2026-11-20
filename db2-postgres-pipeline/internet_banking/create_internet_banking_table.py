#!/usr/bin/env python3
"""
Create internetBanking table in PostgreSQL
Based on internet-banking.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_internet_banking_table():
    """Create the internetBanking table in PostgreSQL"""

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
        logger.info("Dropping existing internetBanking table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "internetBanking" CASCADE')

        # Create internetBanking table
        logger.info("Creating internetBanking table...")
        create_table_sql = """
        CREATE TABLE "internetBanking" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "transactionDate" VARCHAR(12),
            "accountNumber" VARCHAR(50),
            "customerIdentificationNumber" VARCHAR(50),
            "beneficiaryName" VARCHAR(255),
            "beneficiaryCountry" VARCHAR(100),
            "benBankOrWalletCode" VARCHAR(50),
            "benAccountOrMobileNumber" VARCHAR(50),
            "serviceCategory" VARCHAR(100),
            "subServiceCategory" VARCHAR(100),
            "serviceStatus" VARCHAR(50),
            "transactionRef" VARCHAR(255),
            "currency" VARCHAR(10),
            "orgAmount" VARCHAR(50),
            "usdAmount" VARCHAR(50),
            "tzsAmount" VARCHAR(50),
            "valueAddedTaxAmount" VARCHAR(50),
            "exciseDutyAmount" VARCHAR(50),
            "electronicLevyAmount" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_table_sql)

        # Create indexes for better performance
        logger.info("Creating indexes...")

        indexes = [
            'CREATE INDEX idx_internetBanking_reporting_date ON "internetBanking"("reportingDate")',
            'CREATE UNIQUE INDEX idx_internetBanking_transaction_ref ON "internetBanking"("transactionRef")',
            'CREATE INDEX idx_internetBanking_transaction_date ON "internetBanking"("transactionDate")',
            'CREATE INDEX idx_internetBanking_account_number ON "internetBanking"("accountNumber")',
            'CREATE INDEX idx_internetBanking_customer_id ON "internetBanking"("customerIdentificationNumber")',
            'CREATE INDEX idx_internetBanking_currency ON "internetBanking"("currency")',
            'CREATE INDEX idx_internetBanking_service_category ON "internetBanking"("serviceCategory")',
            'CREATE INDEX idx_internetBanking_created_at ON "internetBanking"(created_at)'
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
            WHERE table_name = 'internetBanking'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()

        logger.info("Internet Banking table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<40} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)

        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<40} {data_type:<20} {max_len_str:<12} {nullable:<10}")

        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")

        cursor.close()
        conn.close()

        logger.info("internetBanking table setup completed successfully!")

    except Exception as e:
        logger.error(f"Error creating internetBanking table: {e}")
        raise

if __name__ == "__main__":
    create_internet_banking_table()
