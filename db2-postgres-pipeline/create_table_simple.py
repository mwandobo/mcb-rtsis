#!/usr/bin/env python3
"""
Simple table creation script
"""

import psycopg2
from config import Config

def create_table():
    """Create the interbankLoanPayable table"""
    
    config = Config()
    
    try:
        # PostgreSQL connection
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = pg_conn.cursor()
        
        # Create table
        create_sql = '''
        CREATE TABLE IF NOT EXISTS "interbankLoanPayable" (
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
        
        cursor.execute(create_sql)
        pg_conn.commit()
        
        print("✅ Table 'interbankLoanPayable' created successfully")
        
        cursor.close()
        pg_conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_table()