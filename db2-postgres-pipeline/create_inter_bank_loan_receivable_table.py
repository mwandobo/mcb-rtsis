#!/usr/bin/env python3
"""
Create Inter-Bank Loan Receivable Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_inter_bank_loan_receivable_table():
    """Create the inter-bank loan receivable table in PostgreSQL"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        logger.info("🗑️ Dropping existing interBankLoanReceivable table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "interBankLoanReceivable" CASCADE;')
        
        # Create Inter-Bank Loan Receivable table
        logger.info("🏗️ Creating interBankLoanReceivable table...")
        create_table_sql = """
        CREATE TABLE "interBankLoanReceivable" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "customerIdentificationNumber" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "clientName" VARCHAR(200),
            "borrowerCountry" VARCHAR(10),
            "ratingStatus" VARCHAR(50),
            "crRatingBorrower" VARCHAR(50),
            "gradesUnratedBanks" VARCHAR(50),
            "gender" VARCHAR(20),
            "disability" VARCHAR(20),
            "clientType" VARCHAR(50),
            "clientSubType" VARCHAR(50),
            "groupName" VARCHAR(200),
            "groupCode" VARCHAR(50),
            "relatedParty" VARCHAR(50),
            "relationshipCategory" VARCHAR(50),
            "loanNumber" VARCHAR(50),
            "loanType" VARCHAR(100),
            "loanEconomicActivity" VARCHAR(100),
            "loanPhase" VARCHAR(50),
            "transferStatus" VARCHAR(50),
            "purposeMortgage" VARCHAR(100),
            "purposeOtherLoans" VARCHAR(200),
            "sourceFundMortgage" VARCHAR(100),
            "amortizationType" VARCHAR(100),
            "branchCode" VARCHAR(50),
            "loanOfficer" VARCHAR(200),
            "loanSupervisor" VARCHAR(200),
            "groupVillageNumber" VARCHAR(50),
            "cycleNumber" VARCHAR(50),
            "loanInstallment" INTEGER,
            "repaymentFrequency" VARCHAR(50),
            "currency" VARCHAR(10),
            "contractDate" DATE,
            "orgSanctionedAmount" DECIMAL(18,2),
            "usdSanctionedAmount" DECIMAL(18,2),
            "tzsSanctionedAmount" DECIMAL(18,2),
            "orgDisbursedAmount" DECIMAL(18,2),
            "usdDisbursedAmount" DECIMAL(18,2),
            "tzsDisbursedAmount" DECIMAL(18,2),
            "disbursementDate" DATE,
            "maturityDate" DATE,
            "realEndDate" DATE,
            "orgOutstandingPrincipalAmount" DECIMAL(18,2),
            "usdOutstandingPrincipalAmount" DECIMAL(18,2),
            "tzsOutstandingPrincipalAmount" DECIMAL(18,2),
            "orgInstallmentAmount" DECIMAL(18,2),
            "usdInstallmentAmount" DECIMAL(18,2),
            "tzsInstallmentAmount" DECIMAL(18,2),
            "loanInstallmentPaid" INTEGER,
            "gracePeriodPaymentPrincipal" INTEGER,
            "primeLendingRate" DECIMAL(8,4),
            "interestPricingMethod" VARCHAR(100),
            "annualInterestRate" DECIMAL(8,4),
            "effectiveAnnualInterestRate" DECIMAL(8,4),
            "loanFlagType" VARCHAR(50),
            "restructuringDate" DATE,
            "pastDueDays" INTEGER,
            "pastDueAmount" DECIMAL(18,2),
            "internalRiskGroup" VARCHAR(50),
            "orgAccruedInterestAmount" DECIMAL(18,2),
            "usdAccruedInterestAmount" DECIMAL(18,2),
            "tzsAccruedInterestAmount" DECIMAL(18,2),
            "orgPenaltyChargedAmount" DECIMAL(18,2),
            "usdPenaltyChargedAmount" DECIMAL(18,2),
            "tzsPenaltyChargedAmount" DECIMAL(18,2),
            "orgPenaltyPaidAmount" DECIMAL(18,2),
            "usdPenaltyPaidAmount" DECIMAL(18,2),
            "tzsPenaltyPaidAmount" DECIMAL(18,2),
            "orgLoanFeesChargedAmount" DECIMAL(18,2),
            "usdLoanFeesChargedAmount" DECIMAL(18,2),
            "tzsLoanFeesChargedAmount" DECIMAL(18,2),
            "orgLoanFeesPaidAmount" DECIMAL(18,2),
            "usdLoanFeesPaidAmount" DECIMAL(18,2),
            "tzsLoanFeesPaidAmount" DECIMAL(18,2),
            "orgTotMonthlyPaymentAmount" DECIMAL(18,2),
            "usdTotMonthlyPaymentAmount" DECIMAL(18,2),
            "tzsTotMonthlyPaymentAmount" DECIMAL(18,2),
            "sectorSnaClassification" VARCHAR(200),
            "assetClassificationCategory" VARCHAR(50),
            "negStatusContract" VARCHAR(50),
            "customerRole" VARCHAR(50),
            "allowanceProbableLoss" DECIMAL(18,2),
            "botProvision" DECIMAL(18,2),
            "tradingIntent" VARCHAR(50),
            "orgSuspendedInterest" DECIMAL(18,2),
            "usdSuspendedInterest" DECIMAL(18,2),
            "tzsSuspendedInterest" DECIMAL(18,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_customer ON "interBankLoanReceivable"("customerIdentificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_account ON "interBankLoanReceivable"("accountNumber");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_number ON "interBankLoanReceivable"("loanNumber");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_client_name ON "interBankLoanReceivable"("clientName");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_type ON "interBankLoanReceivable"("loanType");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_currency ON "interBankLoanReceivable"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_contract_date ON "interBankLoanReceivable"("contractDate");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_maturity_date ON "interBankLoanReceivable"("maturityDate");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_branch_code ON "interBankLoanReceivable"("branchCode");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_outstanding ON "interBankLoanReceivable"("orgOutstandingPrincipalAmount");',
            'CREATE INDEX IF NOT EXISTS idx_inter_bank_loan_reporting_date ON "interBankLoanReceivable"("reportingDate");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Inter-bank loan receivable table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'interBankLoanReceivable' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for col_name, data_type, max_length in columns[:10]:  # Show first 10 columns
            length_info = f"({max_length})" if max_length else ""
            logger.info(f"  {col_name}: {data_type}{length_info}")
        logger.info(f"  ... and {len(columns)-10} more columns")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to create inter-bank loan receivable table: {e}")
        raise

if __name__ == "__main__":
    create_inter_bank_loan_receivable_table()