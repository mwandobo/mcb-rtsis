#!/usr/bin/env python3
"""
Create loan_information table in PostgreSQL
Based on loan-information-v10.sql structure (92 fields)

Deliberately a separate table from the production "loanInformation" table
(which loan-information-v9.sql / the "loans" pipeline populates and which
has already passed BOT) - this lets the v10 officer/supervisor fix be
verified independently without touching what's already submitted.
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_loan_information_table():
    """Create the loan_information table in PostgreSQL"""

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
        logger.info("Dropping existing loan_information table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "loan_information" CASCADE')

        # Create loan_information table
        logger.info("Creating loan_information table...")
        create_table_sql = """
        CREATE TABLE "loan_information" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "customerIdentificationNumber" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "clientName" VARCHAR(255),
            "borrowerCountry" VARCHAR(100),
            "ratingStatus" VARCHAR(50),
            "crRatingBorrower" VARCHAR(50),
            "gradesUnratedBanks" VARCHAR(50),
            "borrowerCategory" VARCHAR(50),
            "gender" VARCHAR(20),
            "disability" VARCHAR(50),
            "clientType" VARCHAR(50),
            "clientSubType" VARCHAR(50),
            "groupName" VARCHAR(255),
            "groupCode" VARCHAR(50),
            "relatedParty" VARCHAR(50),
            "relationshipCategory" VARCHAR(50),
            "loanNumber" VARCHAR(50),
            "loanType" VARCHAR(100),
            "loanEconomicActivity" VARCHAR(100),
            "loanPhase" VARCHAR(50),
            "transferStatus" VARCHAR(50),
            "purposeMortgage" VARCHAR(100),
            "purposeOtherLoans" VARCHAR(255),
            "sourceFundMortgage" VARCHAR(100),
            "amortizationType" VARCHAR(50),
            "branchCode" VARCHAR(50),
            "loanOfficer" VARCHAR(255),
            "loanSupervisor" VARCHAR(255),
            "groupVillageNumber" VARCHAR(50),
            "cycleNumber" INTEGER,
            "loanInstallment" INTEGER,
            "repaymentFrequency" VARCHAR(50),
            "currency" VARCHAR(10),
            "contractDate" VARCHAR(12),
            "orgSanctionAmount" VARCHAR(50),
            "usdSanctionAmount" VARCHAR(50),
            "tzsSanctionAmount" VARCHAR(50),
            "orgDisbursedAmount" VARCHAR(50),
            "usdDisbursedAmount" VARCHAR(50),
            "tzsDisbursedAmount" VARCHAR(50),
            "disbursementDate" VARCHAR(12),
            "maturityDate" VARCHAR(12),
            "realEndDate" VARCHAR(12),
            "orgOutstandingPrincipalAmount" VARCHAR(50),
            "usdOutstandingPrincipalAmount" VARCHAR(50),
            "tzsOutstandingPrincipalAmount" VARCHAR(50),
            "orgInstallmentAmount" VARCHAR(50),
            "usdInstallmentAmount" VARCHAR(50),
            "tzsInstallmentAmount" VARCHAR(50),
            "loanInstallmentPaid" INTEGER,
            "gracePeriodPaymentPrincipal" VARCHAR(50),
            "primeLendingRate" VARCHAR(50),
            "interestPricingMethod" VARCHAR(50),
            "annualInterestRate" VARCHAR(50),
            "effectiveAnnualInterestRate" VARCHAR(50),
            "firstInstallmentPaymentDate" VARCHAR(12),
            "lastPaymentDate" VARCHAR(50),
            "collateralPledged" JSONB,
            "loanFlagType" VARCHAR(50),
            "restructuringDate" VARCHAR(12),
            "pastDueDays" INTEGER,
            "pastDueAmount" VARCHAR(50),
            "internalRiskGroup" VARCHAR(50),
            "orgAccruedInterestAmount" VARCHAR(50),
            "usdAccruedInterestAmount" VARCHAR(50),
            "tzsAccruedInterestAmount" VARCHAR(50),
            "orgPenaltyChargedAmount" VARCHAR(50),
            "usdPenaltyChargedAmount" VARCHAR(50),
            "tzsPenaltyChargedAmount" VARCHAR(50),
            "orgPenaltyPaidAmount" VARCHAR(50),
            "usdPenaltyPaidAmount" VARCHAR(50),
            "tzsPenaltyPaidAmount" VARCHAR(50),
            "orgLoanFeesChargedAmount" VARCHAR(50),
            "usdLoanFeesChargedAmount" VARCHAR(50),
            "tzsLoanFeesChargedAmount" VARCHAR(50),
            "orgLoanFeesPaidAmount" VARCHAR(50),
            "usdLoanFeesPaidAmount" VARCHAR(50),
            "tzsLoanFeesPaidAmount" VARCHAR(50),
            "orgTotMonthlyPaymentAmount" VARCHAR(50),
            "usdTotMonthlyPaymentAmount" VARCHAR(50),
            "tzsTotMonthlyPaymentAmount" VARCHAR(50),
            "sectorSnaClassification" VARCHAR(100),
            "assetClassificationCategory" VARCHAR(100),
            "negStatusContract" VARCHAR(50),
            "customerRole" VARCHAR(50),
            "allowanceProbableLoss" VARCHAR(50),
            "botProvision" VARCHAR(50),
            "tradingIntent" VARCHAR(50),
            "orgSuspendedInterest" VARCHAR(50),
            "usdSuspendedInterest" VARCHAR(50),
            "tzsSuspendedInterest" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_table_sql)

        # Create indexes for better performance
        logger.info("Creating indexes...")

        indexes = [
            'CREATE INDEX idx_loan_information_reporting_date ON "loan_information"("reportingDate")',
            'CREATE UNIQUE INDEX idx_loan_information_loan_number ON "loan_information"("loanNumber")',
            'CREATE INDEX idx_loan_information_customer_id ON "loan_information"("customerIdentificationNumber")',
            'CREATE INDEX idx_loan_information_account_number ON "loan_information"("accountNumber")',
            'CREATE INDEX idx_loan_information_client_name ON "loan_information"("clientName")',
            'CREATE INDEX idx_loan_information_loan_type ON "loan_information"("loanType")',
            'CREATE INDEX idx_loan_information_currency ON "loan_information"("currency")',
            'CREATE INDEX idx_loan_information_branch_code ON "loan_information"("branchCode")',
            'CREATE INDEX idx_loan_information_loan_officer ON "loan_information"("loanOfficer")',
            'CREATE INDEX idx_loan_information_contract_date ON "loan_information"("contractDate")',
            'CREATE INDEX idx_loan_information_created_at ON "loan_information"(created_at)'
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
            WHERE table_name = 'loan_information'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()

        logger.info("Loan Information (refine) table created successfully!")
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

        logger.info("loan_information table setup completed successfully!")

    except Exception as e:
        logger.error(f"Error creating loan_information table: {e}")
        raise

if __name__ == "__main__":
    create_loan_information_table()
