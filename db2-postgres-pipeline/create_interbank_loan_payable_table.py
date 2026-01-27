#!/usr/bin/env python3
"""
Create interbank loan payable table for RTSIS reporting
"""

import psycopg2
from config import Config

def create_interbank_loan_payable_table():
    """Create the interbankLoanPayable table in PostgreSQL"""
    
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
            cursor.execute('DROP TABLE IF EXISTS "interbankLoanPayable" CASCADE')
            
            # Create table with all fields from SQL query (21 fields) - No primary key to allow duplicates
            create_table_sql = '''
            CREATE TABLE "interbankLoanPayable" (
                "reportingDate" TIMESTAMP NOT NULL,
                "lenderName" VARCHAR(200),
                "accountNumber" VARCHAR(100) NOT NULL,
                "lenderCountry" VARCHAR(100),
                "borrowingType" VARCHAR(100),
                "transactionDate" DATE,
                "disbursementDate" DATE,
                "maturityDate" DATE,
                "currency" VARCHAR(10),
                "orgAmountOpening" DECIMAL(15,2),
                "usdAmountOpening" DECIMAL(15,2),
                "tzsAmountOpening" DECIMAL(15,2),
                "orgAmountRepayment" DECIMAL(15,2),
                "usdAmountRepayment" DECIMAL(15,2),
                "tzsAmountRepayment" DECIMAL(15,2),
                "orgAmountClosing" DECIMAL(15,2),
                "usdAmountClosing" DECIMAL(15,2),
                "tzsAmountClosing" DECIMAL(15,2),
                "tenureDays" INTEGER,
                "annualInterestRate" DECIMAL(9,6),
                "interestRateType" VARCHAR(50),
                "originalTimestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
            
            cursor.execute(create_table_sql)
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_reporting_date ON "interbankLoanPayable" ("reportingDate")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_account_number ON "interbankLoanPayable" ("accountNumber")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_lender ON "interbankLoanPayable" ("lenderName")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_currency ON "interbankLoanPayable" ("currency")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_maturity ON "interbankLoanPayable" ("maturityDate")')
            
            pg_conn.commit()
            print("✅ Interbank loan payable table created successfully")
            
    except Exception as e:
        print(f"❌ Error creating interbank loan payable table: {e}")
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()

if __name__ == "__main__":
    create_interbank_loan_payable_table()