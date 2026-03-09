#!/usr/bin/env python3
"""
Create loanInformation table in PostgreSQL
Based on loan-information-v7.sql structure (92 fields)
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_loans_table():
    """Create the loanInformation table in PostgreSQL"""
    
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
        logger.info("Dropping existing loanInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "loanInformation" CASCADE')
        
        # Create loanInformation table
        logger.info("Creating loanInformation table...")
        create_table_sql = """
        CREATE TABLE "loanInformation" (
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
            "orgSanctionedAmount" VARCHAR(50),
            "usdSanctionedAmount" VARCHAR(50),
            "tzsSanctionedAmount" VARCHAR(50),
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
            'CREATE INDEX idx_loaninformation_reporting_date ON "loanInformation"("reportingDate")',
            'CREATE UNIQUE INDEX idx_loaninformation_loan_number ON "loanInformation"("loanNumber")',
            'CREATE INDEX idx_loaninformation_customer_id ON "loanInformation"("customerIdentificationNumber")',
            'CREATE INDEX idx_loaninformation_account_number ON "loanInformation"("accountNumber")',
            'CREATE INDEX idx_loaninformation_client_name ON "loanInformation"("clientName")',
            'CREATE INDEX idx_loaninformation_loan_type ON "loanInformation"("loanType")',
            'CREATE INDEX idx_loaninformation_currency ON "loanInformation"("currency")',
            'CREATE INDEX idx_loaninformation_branch_code ON "loanInformation"("branchCode")',
            'CREATE INDEX idx_loaninformation_loan_officer ON "loanInformation"("loanOfficer")',
            'CREATE INDEX idx_loaninformation_contract_date ON "loanInformation"("contractDate")',
            'CREATE INDEX idx_loaninformation_created_at ON "loanInformation"(created_at)'
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
            WHERE table_name = 'loanInformation'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Loan Information table created successfully!")
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
        
        logger.info("loanInformation table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating loanInformation table: {e}")
        raise

if __name__ == "__main__":
    create_loans_table()
