#!/usr/bin/env python3
"""
Create writtenOffLoans table in PostgreSQL
Based on written-off-loans.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_written_off_loans_table():
    """Create the writtenOffLoans table in PostgreSQL"""

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
        logger.info("Dropping existing writtenOffLoans table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "writtenOffLoans" CASCADE')

        # Create writtenOffLoans table
        logger.info("Creating writtenOffLoans table...")
        create_table_sql = """
        CREATE TABLE "writtenOffLoans" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "customerIdentificationNumber" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "borrowerName" VARCHAR(255),
            "borrowerCountry" VARCHAR(100),
            "clientType" VARCHAR(50),
            "loanNumber" VARCHAR(50),
            "loanType" VARCHAR(50),
            "currency" VARCHAR(10),
            "orgDisbursedAmount" VARCHAR(50),
            "usdDisbursedAmount" VARCHAR(50),
            "tzsDisbursedAmount" VARCHAR(50),
            "disbursementDate" VARCHAR(12),
            "gracePeriodPaymentPrincipal" VARCHAR(20),
            "maturityDate" VARCHAR(12),
            "orgGrossPaidAmount" VARCHAR(50),
            "usdGrossPaidAmount" VARCHAR(50),
            "tzsGrossPaidAmount" VARCHAR(50),
            "writtenOffDate" VARCHAR(12),
            "orgOutstandingPrincipalAmount" VARCHAR(50),
            "usdOutstandingPrincipalAmount" VARCHAR(50),
            "tzsOutstandingPrincipalAmount" VARCHAR(50),
            "annualInterestRate" VARCHAR(20),
            "latestInstallmentPayDate" VARCHAR(12),
            "collateralPledged" VARCHAR(100),
            "orgCollateralValue" VARCHAR(50),
            "usdCollateralValue" VARCHAR(50),
            "tzsCollateralValue" VARCHAR(50),
            "pastDueDays" VARCHAR(20),
            "loanOfficer" VARCHAR(255),
            "loanSupervisor" VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_table_sql)

        # Create indexes for better performance
        logger.info("Creating indexes...")

        indexes = [
            'CREATE INDEX idx_writtenOffLoans_reporting_date ON "writtenOffLoans"("reportingDate")',
            'CREATE UNIQUE INDEX idx_writtenOffLoans_loan_number ON "writtenOffLoans"("loanNumber")',
            'CREATE INDEX idx_writtenOffLoans_account_number ON "writtenOffLoans"("accountNumber")',
            'CREATE INDEX idx_writtenOffLoans_customer_id ON "writtenOffLoans"("customerIdentificationNumber")',
            'CREATE INDEX idx_writtenOffLoans_written_off_date ON "writtenOffLoans"("writtenOffDate")',
            'CREATE INDEX idx_writtenOffLoans_currency ON "writtenOffLoans"("currency")',
            'CREATE INDEX idx_writtenOffLoans_loan_type ON "writtenOffLoans"("loanType")',
            'CREATE INDEX idx_writtenOffLoans_created_at ON "writtenOffLoans"(created_at)'
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
            WHERE table_name = 'writtenOffLoans'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()

        logger.info("Written Off Loans table created successfully!")
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

        logger.info("writtenOffLoans table setup completed successfully!")

    except Exception as e:
        logger.error(f"Error creating writtenOffLoans table: {e}")
        raise

if __name__ == "__main__":
    create_written_off_loans_table()
