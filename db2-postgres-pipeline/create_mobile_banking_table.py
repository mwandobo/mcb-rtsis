#!/usr/bin/env python3
"""
Create mobileBanking table with camelCase naming
"""

import psycopg2
from config import Config

def create_mobile_banking_table():
    """Create the mobileBanking table with camelCase field names"""
    
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
        
        print("🏦 CREATING MOBILE BANKING TABLE")
        print("=" * 60)
        
        # Drop table if exists
        cursor.execute('DROP TABLE IF EXISTS "mobileBanking" CASCADE')
        print("🗑️ Dropped existing mobileBanking table (if any)")
        
        # Create mobileBanking table with camelCase naming
        create_table_sql = """
        CREATE TABLE "mobileBanking" (
            "reportingDate" VARCHAR(50),
            "transactionDate" DATE NOT NULL,
            "accountNumber" VARCHAR(50),
            "customerIdentificationNumber" VARCHAR(50),
            "mobileTransactionType" VARCHAR(50) NOT NULL,
            "serviceCategory" VARCHAR(100) NOT NULL,
            "subServiceCategory" VARCHAR(100),
            "serviceStatus" VARCHAR(50) NOT NULL,
            "transactionRef" VARCHAR(200) PRIMARY KEY,
            "benBankOrWalletCode" VARCHAR(50) NOT NULL,
            "benAccountOrMobileNumber" VARCHAR(50),
            "currency" VARCHAR(10) NOT NULL,
            "orgAmount" DECIMAL(18,2) NOT NULL,
            "tzsAmount" DECIMAL(18,2),
            "valueAddedTaxAmount" DECIMAL(18,2),
            "exciseDutyAmount" DECIMAL(18,2),
            "electronicLevyAmount" DECIMAL(18,2),
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        indexes = [
            'CREATE INDEX "idx_mobileBanking_transactionDate" ON "mobileBanking"("transactionDate")',
            'CREATE INDEX "idx_mobileBanking_mobileTransactionType" ON "mobileBanking"("mobileTransactionType")',
            'CREATE INDEX "idx_mobileBanking_currency" ON "mobileBanking"("currency")',
            'CREATE INDEX "idx_mobileBanking_accountNumber" ON "mobileBanking"("accountNumber")',
            'CREATE INDEX "idx_mobileBanking_customerIdentificationNumber" ON "mobileBanking"("customerIdentificationNumber")'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Create trigger for updatedAt
        trigger_sql = """
        CREATE OR REPLACE FUNCTION update_mobile_banking_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW."updatedAt" = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trigger_mobile_banking_updated_at
            BEFORE UPDATE ON "mobileBanking"
            FOR EACH ROW
            EXECUTE FUNCTION update_mobile_banking_updated_at();
        """
        
        cursor.execute(trigger_sql)
        
        conn.commit()
        
        print("✅ Created mobileBanking table with camelCase fields:")
        print("   📋 Fields:")
        print("      - reportingDate (VARCHAR)")
        print("      - transactionDate (DATE) - NOT NULL")
        print("      - accountNumber (VARCHAR)")
        print("      - customerIdentificationNumber (VARCHAR)")
        print("      - mobileTransactionType (VARCHAR) - NOT NULL")
        print("      - serviceCategory (VARCHAR) - NOT NULL")
        print("      - subServiceCategory (VARCHAR)")
        print("      - serviceStatus (VARCHAR) - NOT NULL")
        print("      - transactionRef (VARCHAR) - PRIMARY KEY")
        print("      - benBankOrWalletCode (VARCHAR) - NOT NULL")
        print("      - benAccountOrMobileNumber (VARCHAR)")
        print("      - currency (VARCHAR) - NOT NULL")
        print("      - orgAmount (DECIMAL) - NOT NULL")
        print("      - tzsAmount (DECIMAL)")
        print("      - valueAddedTaxAmount (DECIMAL)")
        print("      - exciseDutyAmount (DECIMAL)")
        print("      - electronicLevyAmount (DECIMAL)")
        print("      - createdAt (TIMESTAMP)")
        print("      - updatedAt (TIMESTAMP)")
        print()
        print("   🔍 Indexes created for:")
        print("      - transactionDate")
        print("      - mobileTransactionType")
        print("      - currency")
        print("      - accountNumber")
        print("      - customerIdentificationNumber")
        print()
        print("   🔑 Primary Key: transactionRef (now truly unique)")
        print("   🔄 Auto-update trigger: updatedAt")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ MOBILE BANKING TABLE CREATED SUCCESSFULLY!")
        print("🏦 Table: mobileBanking (camelCase)")
        print("📊 Ready for mobile banking data pipeline")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Failed to create mobileBanking table: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_mobile_banking_table()