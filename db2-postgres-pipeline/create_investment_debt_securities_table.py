#!/usr/bin/env python3
"""
Create investment debt securities table for RTSIS reporting
"""

import psycopg2
from config import Config

def create_investment_debt_securities_table():
    """Create the investmentDebtSecurities table in PostgreSQL"""
    
    config = Config()
    
    # PostgreSQL connection
    pg_conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    try:
        with pg_conn.cursor() as cursor:
            # Drop table if exists
            cursor.execute('DROP TABLE IF EXISTS "investmentDebtSecurities" CASCADE')
            
            # Create table with camelCase naming
            create_table_sql = '''
            CREATE TABLE "investmentDebtSecurities" (
                "reportingDate" TIMESTAMP NOT NULL,
                "securityNumber" VARCHAR(100) NOT NULL,
                "securityType" VARCHAR(100),
                "securityIssuerName" VARCHAR(200),
                "externalIssuerRatting" VARCHAR(50),
                "gradesUnratedBanks" VARCHAR(50),
                "securityIssuerCountry" VARCHAR(100),
                "snaIssuerSector" VARCHAR(100),
                "currency" VARCHAR(10),
                "orgCostValueAmount" DECIMAL(15,2),
                "tzsCostValueAmount" DECIMAL(15,2),
                "usdCostValueAmount" DECIMAL(15,2),
                "orgFaceValueAmount" DECIMAL(15,2),
                "tzsgFaceValueAmount" DECIMAL(15,2),
                "usdgFaceValueAmount" DECIMAL(15,2),
                "orgFairValueAmount" DECIMAL(15,2),
                "tzsgFairValueAmount" DECIMAL(15,2),
                "usdgFairValueAmount" DECIMAL(15,2),
                "interestRate" DECIMAL(9,6),
                "purchaseDate" DATE,
                "valueDate" DATE,
                "maturityDate" DATE,
                "tradingIntent" VARCHAR(50),
                "securityEncumbaranceStatus" VARCHAR(50),
                "pastDueDays" INTEGER,
                "allowanceProbableLoss" DECIMAL(15,2),
                "assetClassificationCategory" INTEGER,
                "originalTimestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY ("securityNumber")
            )
            '''
            
            cursor.execute(create_table_sql)
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_investment_debt_securities_reporting_date ON "investmentDebtSecurities" ("reportingDate")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_investment_debt_securities_security_type ON "investmentDebtSecurities" ("securityType")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_investment_debt_securities_issuer ON "investmentDebtSecurities" ("securityIssuerName")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_investment_debt_securities_currency ON "investmentDebtSecurities" ("currency")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_investment_debt_securities_maturity ON "investmentDebtSecurities" ("maturityDate")')
            
            pg_conn.commit()
            print("✅ Investment debt securities table created successfully")
            
    except Exception as e:
        print(f"❌ Error creating investment debt securities table: {e}")
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()

if __name__ == "__main__":
    create_investment_debt_securities_table()