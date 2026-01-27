#!/usr/bin/env python3
"""
Create PostgreSQL table for overdraft records
"""

import psycopg2
from config import Config

def create_overdraft_table():
    """Create the overdraft table in PostgreSQL"""
    config = Config()
    
    try:
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
        
        # Create overdraft table with all 79 fields
        create_table_query = """
        CREATE TABLE overdraft (
            id SERIAL PRIMARY KEY,
            reportingDate VARCHAR(50),
            custId VARCHAR(50),
            accountNumber VARCHAR(50),
            customerIdentificationNumber VARCHAR(50),
            clientName VARCHAR(255),
            clientType VARCHAR(100),
            borrowerCountry VARCHAR(100),
            ratingStatus INTEGER,
            crRatingBorrower VARCHAR(100),
            gradesUnratedBanks VARCHAR(100),
            groupCode VARCHAR(100),
            relatedEntityName VARCHAR(255),
            relatedParty VARCHAR(100),
            relationshipCategory VARCHAR(100),
            loanProductType VARCHAR(255),
            idProduct VARCHAR(50),
            overdraftEconomicActivity VARCHAR(100),
            loanPhase VARCHAR(100),
            transferStatus VARCHAR(100),
            purposeOtherLoans VARCHAR(100),
            contractDate DATE,
            branchCode VARCHAR(50),
            loanOfficer VARCHAR(255),
            loanSupervisor VARCHAR(255),
            currency VARCHAR(10),
            orgSanctionedAmount DECIMAL(15,2),
            usdSanctionedAmount DECIMAL(15,2),
            tzsSanctionedAmount DECIMAL(15,2),
            orgUtilisedAmount DECIMAL(15,2),
            usdUtilisedAmount DECIMAL(15,2),
            tzsUtilisedAmount DECIMAL(15,2),
            orgCrUsageLast30DaysAmount DECIMAL(15,2),
            usdCrUsageLast30DaysAmount DECIMAL(15,2),
            tzsCrUsageLast30DaysAmount DECIMAL(15,2),
            disbursementDate DATE,
            expiryDate DATE,
            realEndDate DATE,
            orgOutstandingAmount DECIMAL(15,2),
            usdOutstandingAmount DECIMAL(15,2),
            tzsOutstandingAmount DECIMAL(15,2),
            orgOutstandingPrincipalAmount DECIMAL(15,2),
            usdOutstandingPrincipalAmount DECIMAL(15,2),
            tzsOutstandingPrincipalAmount DECIMAL(15,2),
            latestCustomerCreditDate DATE,
            latestCreditAmount DECIMAL(15,2),
            primeLendingRate DECIMAL(8,4),
            annualInterestRate DECIMAL(8,4),
            collateralPledged VARCHAR(255),
            orgCollateralValue DECIMAL(15,2),
            usdCollateralValue DECIMAL(15,2),
            tzsCollateralValue DECIMAL(15,2),
            restructuredLoans INTEGER,
            orgAccruedInterestAmount DECIMAL(15,2),
            usdAccruedInterestAmount DECIMAL(15,2),
            tzsAccruedInterestAmount DECIMAL(15,2),
            orgPenaltyChargedAmount DECIMAL(15,2),
            usdPenaltyChargedAmount DECIMAL(15,2),
            tzsPenaltyChargedAmount DECIMAL(15,2),
            orgPenaltyPaidAmount DECIMAL(15,2),
            usdPenaltyPaidAmount DECIMAL(15,2),
            tzsPenaltyPaidAmount DECIMAL(15,2),
            orgLoanFeesChargedAmount DECIMAL(15,2),
            usdLoanFeesChargedAmount DECIMAL(15,2),
            tzsLoanFeesChargedAmount DECIMAL(15,2),
            orgLoanFeesPaidAmount DECIMAL(15,2),
            usdLoanFeesPaidAmount DECIMAL(15,2),
            tzsLoanFeesPaidAmount DECIMAL(15,2),
            orgTotMonthlyPaymentAmount DECIMAL(15,2),
            usdTotMonthlyPaymentAmount DECIMAL(15,2),
            tzsTotMonthlyPaymentAmount DECIMAL(15,2),
            orgInterestPaidTotal DECIMAL(15,2),
            usdInterestPaidTotal DECIMAL(15,2),
            tzsInterestPaidTotal DECIMAL(15,2),
            assetClassificationCategory VARCHAR(100),
            sectorSnaClassification VARCHAR(100),
            negStatusContract VARCHAR(50),
            customerRole VARCHAR(100),
            allowanceProbableLoss INTEGER,
            botProvision INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_overdraft_account_number ON overdraft(accountNumber)")
        cursor.execute("CREATE INDEX idx_overdraft_cust_id ON overdraft(custId)")
        cursor.execute("CREATE INDEX idx_overdraft_currency ON overdraft(currency)")
        cursor.execute("CREATE INDEX idx_overdraft_contract_date ON overdraft(contractDate)")
        cursor.execute("CREATE INDEX idx_overdraft_created_at ON overdraft(created_at)")
        
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
        print(f"\nTable structure ({len(columns)} columns):")
        for col in columns:
            print(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        conn.close()
        
    except Exception as e:
        print(f"Error creating overdraft table: {e}")
        raise

if __name__ == "__main__":
    create_overdraft_table()