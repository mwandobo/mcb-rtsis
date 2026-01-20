#!/usr/bin/env python3
"""
Create deposits table with camelCase naming
"""

import psycopg2
from config import Config

def create_deposits_table():
    """Create the deposits table with camelCase field names"""
    
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
        
        print("🏦 CREATING DEPOSITS TABLE")
        print("=" * 60)
        
        # Drop table if exists
        cursor.execute('DROP TABLE IF EXISTS "deposits" CASCADE')
        print("🗑️ Dropped existing deposits table (if any)")
        
        # Create deposits table with camelCase naming
        create_table_sql = """
        CREATE TABLE "deposits" (
            "reportingDate" VARCHAR(50),
            "clientIdentificationNumber" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            "accountName" VARCHAR(200),
            "customerCategory" VARCHAR(50),
            "customerCountry" VARCHAR(100) DEFAULT 'TANZANIA, UNITED REPUBLIC OF',
            "branchCode" VARCHAR(50),
            "clientType" VARCHAR(50),
            "relationshipType" VARCHAR(100) DEFAULT 'Domestic banks unrelated',
            "district" VARCHAR(100),
            "region" VARCHAR(100) DEFAULT 'DAR ES SALAAM',
            "accountProductName" VARCHAR(200),
            "accountType" VARCHAR(50) DEFAULT 'Saving',
            "accountSubType" VARCHAR(50),
            "depositCategory" VARCHAR(100) DEFAULT 'Deposit from public',
            "depositAccountStatus" VARCHAR(50) NOT NULL,
            "transactionUniqueRef" VARCHAR(200) PRIMARY KEY,
            "timeStamp" VARCHAR(50),
            "serviceChannel" VARCHAR(50) DEFAULT 'Branch',
            "currency" VARCHAR(10) NOT NULL,
            "transactionType" VARCHAR(50) DEFAULT 'Deposit',
            "orgTransactionAmount" DECIMAL(18,2) NOT NULL,
            "usdTransactionAmount" DECIMAL(18,2),
            "tzsTransactionAmount" DECIMAL(18,2) NOT NULL,
            "transactionPurposes" TEXT,
            "sectorSnaClassification" VARCHAR(100),
            "lienNumber" VARCHAR(50),
            "orgAmountLien" DECIMAL(18,2),
            "usdAmountLien" DECIMAL(18,2),
            "tzsAmountLien" DECIMAL(18,2),
            "contractDate" DATE,
            "maturityDate" DATE,
            "annualInterestRate" DECIMAL(10,4),
            "interestRateType" VARCHAR(50),
            "orgInterestAmount" DECIMAL(18,2) DEFAULT 0,
            "usdInterestAmount" DECIMAL(18,2) DEFAULT 0,
            "tzsInterestAmount" DECIMAL(18,2) DEFAULT 0,
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        indexes = [
            'CREATE INDEX "idx_deposits_reportingDate" ON "deposits"("reportingDate")',
            'CREATE INDEX "idx_deposits_accountNumber" ON "deposits"("accountNumber")',
            'CREATE INDEX "idx_deposits_clientIdentificationNumber" ON "deposits"("clientIdentificationNumber")',
            'CREATE INDEX "idx_deposits_currency" ON "deposits"("currency")',
            'CREATE INDEX "idx_deposits_transactionType" ON "deposits"("transactionType")',
            'CREATE INDEX "idx_deposits_depositAccountStatus" ON "deposits"("depositAccountStatus")',
            'CREATE INDEX "idx_deposits_branchCode" ON "deposits"("branchCode")',
            'CREATE INDEX "idx_deposits_contractDate" ON "deposits"("contractDate")'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Create trigger for updatedAt
        trigger_sql = """
        CREATE OR REPLACE FUNCTION update_deposits_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW."updatedAt" = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trigger_deposits_updated_at
            BEFORE UPDATE ON "deposits"
            FOR EACH ROW
            EXECUTE FUNCTION update_deposits_updated_at();
        """
        
        cursor.execute(trigger_sql)
        
        conn.commit()
        
        print("✅ Created deposits table with camelCase fields:")
        print("   📋 Fields:")
        print("      - reportingDate (VARCHAR)")
        print("      - clientIdentificationNumber (VARCHAR)")
        print("      - accountNumber (VARCHAR)")
        print("      - accountName (VARCHAR)")
        print("      - customerCategory (VARCHAR)")
        print("      - customerCountry (VARCHAR) - DEFAULT")
        print("      - branchCode (VARCHAR)")
        print("      - clientType (VARCHAR)")
        print("      - relationshipType (VARCHAR) - DEFAULT")
        print("      - district (VARCHAR)")
        print("      - region (VARCHAR) - DEFAULT")
        print("      - accountProductName (VARCHAR)")
        print("      - accountType (VARCHAR) - DEFAULT")
        print("      - accountSubType (VARCHAR)")
        print("      - depositCategory (VARCHAR) - DEFAULT")
        print("      - depositAccountStatus (VARCHAR) - NOT NULL")
        print("      - transactionUniqueRef (VARCHAR) - PRIMARY KEY")
        print("      - timeStamp (VARCHAR)")
        print("      - serviceChannel (VARCHAR) - DEFAULT")
        print("      - currency (VARCHAR) - NOT NULL")
        print("      - transactionType (VARCHAR) - DEFAULT")
        print("      - orgTransactionAmount (DECIMAL) - NOT NULL")
        print("      - usdTransactionAmount (DECIMAL)")
        print("      - tzsTransactionAmount (DECIMAL) - NOT NULL")
        print("      - transactionPurposes (TEXT)")
        print("      - sectorSnaClassification (VARCHAR)")
        print("      - lienNumber (VARCHAR)")
        print("      - orgAmountLien (DECIMAL)")
        print("      - usdAmountLien (DECIMAL)")
        print("      - tzsAmountLien (DECIMAL)")
        print("      - contractDate (DATE)")
        print("      - maturityDate (DATE)")
        print("      - annualInterestRate (DECIMAL)")
        print("      - interestRateType (VARCHAR)")
        print("      - orgInterestAmount (DECIMAL) - DEFAULT 0")
        print("      - usdInterestAmount (DECIMAL) - DEFAULT 0")
        print("      - tzsInterestAmount (DECIMAL) - DEFAULT 0")
        print("      - createdAt (TIMESTAMP)")
        print("      - updatedAt (TIMESTAMP)")
        print()
        print("   🔍 Indexes created for:")
        print("      - reportingDate")
        print("      - accountNumber")
        print("      - clientIdentificationNumber")
        print("      - currency")
        print("      - transactionType")
        print("      - depositAccountStatus")
        print("      - branchCode")
        print("      - contractDate")
        print()
        print("   🔑 Primary Key: transactionUniqueRef")
        print("   🔄 Auto-update trigger: updatedAt")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ DEPOSITS TABLE CREATED SUCCESSFULLY!")
        print("🏦 Table: deposits (camelCase)")
        print("📊 Ready for deposits data pipeline")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Failed to create deposits table: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_deposits_table()