#!/usr/bin/env python3
"""
Create PostgreSQL table for overdraft records
Based on overdraft-v3.sql query structure
"""

import sys
import os
import psycopg2

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def create_overdraft_table():
    """Create the overdraft table in PostgreSQL"""
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
        cursor.execute("DROP TABLE IF EXISTS overdraft CASCADE")
        print("Dropped existing overdraft table")
        
        # Create overdraft table based on overdraft-v3.sql structure
        create_table_query = """
        CREATE TABLE overdraft (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "customerIdentificationNumber" VARCHAR(50),
            "clientName" VARCHAR(255),
            "clientType" VARCHAR(50),
            "borrowerCountry" VARCHAR(100),
            "ratingStatus" INTEGER,
            "crRatingBorrower" VARCHAR(50),
            "gradesUnratedBanks" VARCHAR(50),
            "groupCode" VARCHAR(50),
            "relatedEntityName" VARCHAR(255),
            "relatedParty" VARCHAR(50),
            "relationshipCategory" VARCHAR(50),
            "loanProductType" VARCHAR(255),
            "overdraftEconomicActivity" VARCHAR(100),
            "loanPhase" VARCHAR(100),
            "transferStatus" VARCHAR(100),
            "purposeOtherLoans" VARCHAR(100),
            "contractDate" VARCHAR(50),
            "branchCode" VARCHAR(50),
            "loanOfficer" VARCHAR(255),
            "loanSupervisor" VARCHAR(255),
            "currency" VARCHAR(10),
            "orgSanctionedAmount" DECIMAL(18,2),
            "usdSanctionedAmount" DECIMAL(18,2),
            "tzsSanctionedAmount" DECIMAL(18,2),
            "orgUtilisedAmount" DECIMAL(18,2),
            "usdUtilisedAmount" DECIMAL(18,2),
            "tzsUtilisedAmount" DECIMAL(18,2),
            "orgCrUsageLast30DaysAmount" DECIMAL(18,2),
            "usdCrUsageLast30DaysAmount" DECIMAL(18,2),
            "tzsCrUsageLast30DaysAmount" DECIMAL(18,2),
            "disbursementDate" VARCHAR(50),
            "expiryDate" VARCHAR(50),
            "realEndDate" VARCHAR(50),
            "orgOutstandingAmount" DECIMAL(18,2),
            "usdOutstandingAmount" DECIMAL(18,2),
            "tzsOutstandingAmount" DECIMAL(18,2),
            "orgOutstandingPrincipalAmount" DECIMAL(18,2),
            "usdOutstandingPrincipalAmount" DECIMAL(18,2),
            "tzsOutstandingPrincipalAmount" DECIMAL(18,2),
            "latestCustomerCreditDate" VARCHAR(50),
            "latestCreditAmount" DECIMAL(18,2),
            "primeLendingRate" DECIMAL(8,4),
            "annualInterestRate" DECIMAL(8,4),
            "collateralPledged" JSONB,
            "restructuredLoans" INTEGER,
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
            "orgInterestPaidTotal" DECIMAL(18,2),
            "usdInterestPaidTotal" DECIMAL(18,2),
            "tzsInterestPaidTotal" DECIMAL(18,2),
            "assetClassificationCategory" VARCHAR(100),
            "sectorSnaClassification" VARCHAR(255),
            "negStatusContract" VARCHAR(100),
            "customerRole" VARCHAR(100),
            "allowanceProbableLoss" INTEGER,
            "botProvision" INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_overdraft_account_number ON overdraft(\"accountNumber\")")
        cursor.execute("CREATE INDEX idx_overdraft_customer_id ON overdraft(\"customerIdentificationNumber\")")
        cursor.execute("CREATE INDEX idx_overdraft_currency ON overdraft(\"currency\")")
        cursor.execute("CREATE INDEX idx_overdraft_contract_date ON overdraft(\"contractDate\")")
        cursor.execute("CREATE INDEX idx_overdraft_created_at ON overdraft(created_at)")
        cursor.execute("CREATE INDEX idx_overdraft_collateral_gin ON overdraft USING GIN (\"collateralPledged\")")
        
        conn.commit()
        print("Overdraft table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'overdraft' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\nOverdraft table created with {len(columns)} columns:")
        for col in columns:
            print(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating overdraft table: {e}")
        raise


if __name__ == "__main__":
    create_overdraft_table()