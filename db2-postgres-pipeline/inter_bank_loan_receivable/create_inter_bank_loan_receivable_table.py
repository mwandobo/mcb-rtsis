#!/usr/bin/env python3
"""
Create PostgreSQL table for inter-bank loan receivable records
Based on inter-bank-loan-receivable-v4.sql query structure
"""

import sys
import os
import psycopg2

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def create_inter_bank_loan_receivable_table():
    """Create the interBankLoanReceivable table in PostgreSQL"""
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
        cursor.execute("DROP TABLE IF EXISTS \"interBankLoanReceivable\" CASCADE")
        print("Dropped existing interBankLoanReceivable table")
        
        # Drop indexes if they exist
        cursor.execute("DROP INDEX IF EXISTS idx_inter_bank_loan_receivable_loan_number")
        cursor.execute("DROP INDEX IF EXISTS idx_inter_bank_loan_receivable_borrowers_institution")
        cursor.execute("DROP INDEX IF EXISTS idx_inter_bank_loan_receivable_currency")
        cursor.execute("DROP INDEX IF EXISTS idx_inter_bank_loan_receivable_issue_date")
        cursor.execute("DROP INDEX IF EXISTS idx_inter_bank_loan_receivable_created_at")
        
        # Create interBankLoanReceivable table based on inter-bank-loan-receivable-v4.sql structure
        create_table_query = """
        CREATE TABLE "interBankLoanReceivable" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "borrowersInstitutionCode" VARCHAR(50),
            "borrowerCountry" VARCHAR(100),
            "relationshipType" VARCHAR(100),
            "ratingStatus" INTEGER,
            "externalRatingCorrespondentBorrower" VARCHAR(50),
            "gradesUnratedBorrower" VARCHAR(50),
            "loanNumber" VARCHAR(50),
            "loanType" VARCHAR(100),
            "issueDate" VARCHAR(50),
            "loanMaturityDate" VARCHAR(50),
            currency VARCHAR(10),
            "orgLoanAmount" DECIMAL(18,2),
            "usdLoanAmount" DECIMAL(18,2),
            "tzsLoanAmount" DECIMAL(18,2),
            "interestRate" DECIMAL(8,4),
            "orgAccruedInterestAmount" DECIMAL(18,2),
            "usdAccruedInterestAmount" DECIMAL(18,2),
            "tzsAccruedInterestAmount" DECIMAL(18,2),
            "orgSuspendedInterest" DECIMAL(18,2),
            "usdSuspendedInterest" DECIMAL(18,2),
            "tzsSuspendedInterest" DECIMAL(18,2),
            "ovExpDt" VARCHAR(50),
            "pastDueDays" INTEGER,
            "allowanceProbableLoss" INTEGER,
            "botProvision" INTEGER,
            "assetClassificationCategory" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_inter_bank_loan_receivable_loan_number ON \"interBankLoanReceivable\"(\"loanNumber\")")
        cursor.execute("CREATE INDEX idx_inter_bank_loan_receivable_borrowers_institution ON \"interBankLoanReceivable\"(\"borrowersInstitutionCode\")")
        cursor.execute("CREATE INDEX idx_inter_bank_loan_receivable_currency ON \"interBankLoanReceivable\"(currency)")
        cursor.execute("CREATE INDEX idx_inter_bank_loan_receivable_issue_date ON \"interBankLoanReceivable\"(\"issueDate\")")
        cursor.execute("CREATE INDEX idx_inter_bank_loan_receivable_created_at ON \"interBankLoanReceivable\"(created_at)")
        
        conn.commit()
        print("Inter-bank loan receivable table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'interBankLoanReceivable' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\nInter-bank loan receivable table created with {len(columns)} columns:")
        for col in columns:
            print(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating inter-bank loan receivable table: {e}")
        raise


if __name__ == "__main__":
    create_inter_bank_loan_receivable_table()