#!/usr/bin/env python3
"""
Create Overdraft Table in PostgreSQL
"""

import psycopg2
import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config

def create_overdraft_table():
    """Create the overdraft table in PostgreSQL"""
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password,
            port=config.database.pg_port
        )
        
        cursor = conn.cursor()
        
        # Drop table if exists (optional)
        print("🗑️ Dropping existing overdraft table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "overdraft" CASCADE;')
        
        # Create overdraft table
        print("🏗️ Creating overdraft table...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS "overdraft" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20),
            "accountNumber" VARCHAR(50),
            "customerIdentificationNumber" VARCHAR(50),
            "clientName" VARCHAR(200),
            "clientType" VARCHAR(50),
            "borrowerCountry" VARCHAR(50),
            "ratingStatus" VARCHAR(50),
            "crRatingBorrower" VARCHAR(50),
            "gradesUnratedBanks" VARCHAR(50),
            "groupCode" VARCHAR(50),
            "relatedEntityName" VARCHAR(200),
            "relatedParty" VARCHAR(50),
            "relationshipCategory" VARCHAR(50),
            "loanProductType" VARCHAR(100),
            "overdraftEconomicActivity" VARCHAR(100),
            "loanPhase" VARCHAR(50),
            "transferStatus" VARCHAR(50),
            "purposeOtherLoans" VARCHAR(100),
            "contractDate" DATE,
            "branchCode" VARCHAR(20),
            "loanOfficer" VARCHAR(200),
            "loanSupervisor" VARCHAR(200),
            currency VARCHAR(10),
            "orgSanctionedAmount" DECIMAL(15,2),
            "usdSanctionedAmount" DECIMAL(15,2),
            "tzsSanctionedAmount" DECIMAL(15,2),
            "orgUtilisedAmount" DECIMAL(15,2),
            "usdUtilisedAmount" DECIMAL(15,2),
            "tzsUtilisedAmount" DECIMAL(15,2),
            "orgCrUsageLast30DaysAmount" DECIMAL(15,2),
            "usdCrUsageLast30DaysAmount" DECIMAL(15,2),
            "tzsCrUsageLast30DaysAmount" DECIMAL(15,2),
            "disbursementDate" DATE,
            "expiryDate" DATE,
            "realEndDate" DATE,
            "orgOutstandingAmount" DECIMAL(15,2),
            "usdOutstandingAmount" DECIMAL(15,2),
            "tzsOutstandingAmount" DECIMAL(15,2),
            "latestCustomerCreditDate" DATE,
            "latestCreditAmount" DECIMAL(15,2),
            "primeLendingRate" DECIMAL(8,4),
            "annualInterestRate" DECIMAL(8,4),
            "collateralPledged" DECIMAL(15,2),
            "orgCollateralValue" DECIMAL(15,2),
            "usdCollateralValue" DECIMAL(15,2),
            "tzsCollateralValue" DECIMAL(15,2),
            "restructuredLoans" INTEGER,
            "pastDueDays" INTEGER,
            "pastDueAmount" DECIMAL(15,2),
            "orgAccruedInterestAmount" DECIMAL(15,2),
            "usdAccruedInterestAmount" DECIMAL(15,2),
            "tzsAccruedInterestAmount" DECIMAL(15,2),
            "orgPenaltyChargedAmount" DECIMAL(15,2),
            "usdPenaltyChargedAmount" DECIMAL(15,2),
            "tzsPenaltyChargedAmount" DECIMAL(15,2),
            "orgLoanFeesChargedAmount" DECIMAL(15,2),
            "usdLoanFeesChargedAmount" DECIMAL(15,2),
            "tzsLoanFeesChargedAmount" DECIMAL(15,2),
            "orgLoanFeesPaidAmount" DECIMAL(15,2),
            "usdLoanFeesPaidAmount" DECIMAL(15,2),
            "tzsLoanFeesPaidAmount" DECIMAL(15,2),
            "orgTotMonthlyPaymentAmount" DECIMAL(15,2),
            "usdTotMonthlyPaymentAmount" DECIMAL(15,2),
            "tzsTotMonthlyPaymentAmount" DECIMAL(15,2),
            "orgInterestPaidTotal" DECIMAL(15,2),
            "usdInterestPaidTotal" DECIMAL(15,2),
            "tzsInterestPaidTotal" DECIMAL(15,2),
            "assetClassificationCategory" VARCHAR(50),
            "sectorSnaClassification" VARCHAR(100),
            "negStatusContract" VARCHAR(50),
            "customerRole" VARCHAR(50),
            "allowanceProbableLoss" DECIMAL(15,2),
            "botProvision" DECIMAL(15,2),
            "originalTimestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes
        print("📊 Creating indexes...")
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_overdraft_contract ON "overdraft"("contractDate");',
            'CREATE INDEX IF NOT EXISTS idx_overdraft_account ON "overdraft"("accountNumber");',
            'CREATE INDEX IF NOT EXISTS idx_overdraft_customer ON "overdraft"("customerIdentificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_overdraft_reporting_date ON "overdraft"("reportingDate");',
            'CREATE INDEX IF NOT EXISTS idx_overdraft_branch ON "overdraft"("branchCode");',
            'CREATE INDEX IF NOT EXISTS idx_overdraft_currency ON "overdraft"(currency);'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        print("✅ Overdraft table created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'overdraft' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"\n📋 Table structure ({len(columns)} columns):")
        for col in columns:
            print(f"  - {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
            
    except Exception as e:
        print(f"❌ Error creating overdraft table: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_overdraft_table()