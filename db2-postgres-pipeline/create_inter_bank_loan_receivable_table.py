#!/usr/bin/env python3
"""
Create interBankLoanReceivable table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_table():
    """Create the interBankLoanReceivable table"""
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
        
        logging.info("Connected to PostgreSQL")
        
        # Drop table if exists
        cursor.execute('DROP TABLE IF EXISTS "interBankLoanReceivable" CASCADE;')
        logging.info("Dropped existing table (if any)")
        
        # Create table
        create_table_sql = """
        CREATE TABLE "interBankLoanReceivable" (
            "reportingDate" TIMESTAMP NOT NULL,
            "borrowersInstitutionCode" VARCHAR(50),
            "borrowerCountry" VARCHAR(100),
            "relationshipType" VARCHAR(100),
            "ratingStatus" SMALLINT,
            "externalRatingCorrespondentBorrower" VARCHAR(50),
            "gradesUnratedBorrower" VARCHAR(50),
            "loanNumber" VARCHAR(50),
            "loanType" VARCHAR(100),
            "issueDate" DATE,
            "loanMaturityDate" DATE,
            currency VARCHAR(10),
            "orgLoanAmount" NUMERIC(18, 2),
            "usdLoanAmount" NUMERIC(18, 2),
            "tzsLoanAmount" NUMERIC(18, 2),
            "interestRate" NUMERIC(10, 4),
            "orgAccruedInterestAmount" NUMERIC(18, 2),
            "usdAccruedInterestAmount" NUMERIC(18, 2),
            "tzsAccruedInterestAmount" NUMERIC(18, 2),
            "orgSuspendedInterest" NUMERIC(18, 2),
            "usdSuspendedInterest" NUMERIC(18, 2),
            "tzsSuspendedInterest" NUMERIC(18, 2),
            "pastDueDays" INTEGER,
            "allowanceProbableLoss" INTEGER,
            "botProvision" INTEGER,
            "assetClassificationCategory" VARCHAR(50)
        );
        """
        
        cursor.execute(create_table_sql)
        logging.info("Created interBankLoanReceivable table")
        
        # Create indexes
        cursor.execute('CREATE INDEX idx_inter_bank_loan_reporting_date ON "interBankLoanReceivable"("reportingDate");')
        cursor.execute('CREATE INDEX idx_inter_bank_loan_borrower_code ON "interBankLoanReceivable"("borrowersInstitutionCode");')
        cursor.execute('CREATE INDEX idx_inter_bank_loan_currency ON "interBankLoanReceivable"(currency);')
        logging.info("Created indexes")
        
        conn.commit()
        logging.info("Table creation committed successfully")
        
        cursor.close()
        conn.close()
        
        logging.info("✅ interBankLoanReceivable table created successfully!")
        
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise

if __name__ == "__main__":
    create_table()
