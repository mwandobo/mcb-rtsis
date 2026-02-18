#!/usr/bin/env python3
"""
Create loanInformation table in PostgreSQL with proper camelCase
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_loan_information_table():
    """Create the loanInformation table in PostgreSQL with proper camelCase"""
    
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
        
        # Drop table if exists (with proper quoting)
        cursor.execute('DROP TABLE IF EXISTS "loanInformation" CASCADE')
        
        # Create table with proper camelCase column names (quoted for PostgreSQL)
        create_table_sql = """
        CREATE TABLE "loanInformation" (
            "reportingDate" TIMESTAMP,
            "customerIdentificationNumber" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "clientName" VARCHAR(255),
            "borrowerCountry" VARCHAR(10),
            "ratingStatus" VARCHAR(50),
            "crRatingBorrower" VARCHAR(50),
            "gradesUnratedBanks" VARCHAR(50),
            "gender" VARCHAR(10),
            "disability" VARCHAR(10),
            "clientType" VARCHAR(50),
            "clientSubType" VARCHAR(50),
            "groupName" VARCHAR(255),
            "groupCode" VARCHAR(50),
            "relatedParty" VARCHAR(50),
            "relationshipCategory" VARCHAR(50),
            "loanNumber" VARCHAR(50),
            "loanType" VARCHAR(50),
            "loanEconomicActivity" VARCHAR(100),
            "loanPhase" VARCHAR(50),
            "transferStatus" VARCHAR(50),
            "purposeMortgage" VARCHAR(50),
            "purposeOtherLoans" VARCHAR(255),
            "sourceFundMortgage" VARCHAR(50),
            "amortizationType" VARCHAR(50),
            "branchCode" VARCHAR(20),
            "loanOfficer" VARCHAR(255),
            "loanSupervisor" VARCHAR(255),
            "groupVillageNumber" VARCHAR(50),
            "cycleNumber" INTEGER,
            "loanInstallment" INTEGER,
            "repaymentFrequency" VARCHAR(20),
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
            "primeLendingRate" DECIMAL(15,4),
            "interestPricingMethod" VARCHAR(50),
            "annualInterestRate" DECIMAL(15,4),
            "effectiveAnnualInterestRate" DECIMAL(15,4),
            "firstInstallmentPaymentDate" DATE,
            "lastPaymentDate" DATE,
            "collateralPledged" JSONB,
            "loanFlagType" VARCHAR(50),
            "restructuringDate" DATE,
            "pastDueDays" INTEGER,
            "pastDueAmount" DECIMAL(18,2),
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
            "sectorSnaClassification" VARCHAR(255),
            "assetClassificationCategory" VARCHAR(50),
            "negStatusContract" VARCHAR(50),
            "customerRole" VARCHAR(50),
            "allowanceProbableLoss" DECIMAL(18,2),
            "botProvision" DECIMAL(18,2),
            "tradingIntent" VARCHAR(50),
            "orgSuspendedInterest" DECIMAL(18,2),
            "usdSuspendedInterest" DECIMAL(18,2),
            "tzsSuspendedInterest" DECIMAL(18,2),
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes with proper camelCase quoting
        indexes = [
            'CREATE INDEX "idx_loan_info_customer_id" ON "loanInformation"("customerIdentificationNumber");',
            'CREATE INDEX "idx_loan_info_loan_type" ON "loanInformation"("loanType");',
            'CREATE INDEX "idx_loan_info_branch" ON "loanInformation"("branchCode");',
            'CREATE INDEX "idx_loan_info_currency" ON "loanInformation"("currency");',
            'CREATE INDEX "idx_loan_info_contract_date" ON "loanInformation"("contractDate");',
            'CREATE INDEX "idx_loan_info_maturity_date" ON "loanInformation"("maturityDate");',
            'CREATE INDEX "idx_loan_info_past_due" ON "loanInformation"("pastDueDays");',
            'CREATE INDEX "idx_loan_info_reporting_date" ON "loanInformation"("reportingDate");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Create trigger for updatedAt with proper camelCase quoting
        trigger_sql = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW."updatedAt" = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        CREATE TRIGGER update_loan_information_updated_at 
            BEFORE UPDATE ON "loanInformation" 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """
        
        cursor.execute(trigger_sql)
        
        conn.commit()
        logger.info('"loanInformation" table created successfully with proper camelCase columns, indexes and triggers')
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_loan_information_table()