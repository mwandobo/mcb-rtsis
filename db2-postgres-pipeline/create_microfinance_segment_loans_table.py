#!/usr/bin/env python3
"""
Create Microfinance Segment Loans Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_microfinance_segment_loans_table():
    """Create the microfinance segment loans table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing microfinanceSegmentLoans table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "microfinanceSegmentLoans" CASCADE;')
        
        # Create Microfinance Segment Loans table
        logger.info("🏗️ Creating microfinanceSegmentLoans table...")
        create_table_sql = """
        CREATE TABLE "microfinanceSegmentLoans" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20),
            "customerIdentificationNumber" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "clientName" VARCHAR(200),
            "clientType" VARCHAR(50),
            "gender" VARCHAR(20),
            "age" INTEGER,
            "disabilityStatus" VARCHAR(50),
            "loanNumber" VARCHAR(50),
            "loanIndustryClassification" VARCHAR(100),
            "loanSubIndustry" VARCHAR(100),
            "microfinanceLoansType" VARCHAR(100),
            "amortizationType" VARCHAR(50),
            "branchCode" VARCHAR(20),
            "loanOfficer" VARCHAR(200),
            "loanSupervisor" VARCHAR(200),
            "groupVillageNumber" VARCHAR(50),
            "cycleNumber" INTEGER,
            "currency" VARCHAR(10),
            "orgSanctionedAmount" DECIMAL(15,2),
            "usdSanctionedAmount" DECIMAL(15,2),
            "tzsSanctionedAmount" DECIMAL(15,2),
            "orgDisbursedAmount" DECIMAL(15,2),
            "usdDisbursedAmount" DECIMAL(15,2),
            "tzsDisbursedAmount" DECIMAL(15,2),
            "disbursementDate" VARCHAR(20),
            "maturityDate" VARCHAR(20),
            "restructuringDate" VARCHAR(20),
            "writtenOffAmount" DECIMAL(15,2),
            "agreedLoanInstallments" INTEGER,
            "repaymentFrequency" VARCHAR(50),
            "orgOutstandingPrincipalAmount" DECIMAL(15,2),
            "usdOutstandingPrincipalAmount" DECIMAL(15,2),
            "tzsOutstandingPrincipalAmount" DECIMAL(15,2),
            "loanInstallmentPaid" INTEGER,
            "gracePeriodPaymentPrincipal" INTEGER,
            "primeLendingRate" DECIMAL(8,4),
            "annualInterestRate" DECIMAL(8,4),
            "effectiveAnnualInterestRate" DECIMAL(8,4),
            "firstInstallmentPaymentDate" DATE,
            "loanFlagType" VARCHAR(50),
            "pastDueDays" INTEGER,
            "pastDueAmount" DECIMAL(15,2),
            "orgAccruedInterestAmount" DECIMAL(15,2),
            "usdAccruedInterestAmount" DECIMAL(15,2),
            "tzsAccruedInterestAmount" DECIMAL(15,2),
            "assetClassificationCategory" VARCHAR(50),
            "allowanceProbableLoss" DECIMAL(15,2),
            "botProvision" DECIMAL(15,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_microfinance_loans_unique ON "microfinanceSegmentLoans"("accountNumber", "loanNumber");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_customer ON "microfinanceSegmentLoans"("customerIdentificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_account ON "microfinanceSegmentLoans"("accountNumber");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_branch ON "microfinanceSegmentLoans"("branchCode");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_disbursement ON "microfinanceSegmentLoans"("disbursementDate");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_maturity ON "microfinanceSegmentLoans"("maturityDate");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_currency ON "microfinanceSegmentLoans"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_amount ON "microfinanceSegmentLoans"("orgOutstandingPrincipalAmount");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_type ON "microfinanceSegmentLoans"("microfinanceLoansType");',
            'CREATE INDEX IF NOT EXISTS idx_microfinance_loans_officer ON "microfinanceSegmentLoans"("loanOfficer");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Microfinance segment loans table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'microfinanceSegmentLoans' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for col_name, data_type, max_length in columns:
            length_info = f"({max_length})" if max_length else ""
            logger.info(f"  {col_name}: {data_type}{length_info}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to create microfinance segment loans table: {e}")
        raise

if __name__ == "__main__":
    create_microfinance_segment_loans_table()