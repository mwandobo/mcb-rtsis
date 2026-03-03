#!/usr/bin/env python3
"""
Create PostgreSQL table for income statement records
Based on income-statement.sql query structure
"""

import sys
import os
import psycopg2

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def create_income_statement_table():
    """Create the income_statement table in PostgreSQL"""
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
        cursor.execute("DROP TABLE IF EXISTS income_statement CASCADE")
        print("Dropped existing income_statement table")
        
        # Drop indexes if they exist
        cursor.execute("DROP INDEX IF EXISTS idx_income_statement_reporting_date")
        cursor.execute("DROP INDEX IF EXISTS idx_income_statement_created_at")
        cursor.execute("DROP INDEX IF EXISTS idx_income_statement_interest_income_gin")
        cursor.execute("DROP INDEX IF EXISTS idx_income_statement_interest_expenses_gin")
        cursor.execute("DROP INDEX IF EXISTS idx_income_statement_non_core_credits_gin")
        cursor.execute("DROP INDEX IF EXISTS idx_income_statement_non_interest_income_gin")
        cursor.execute("DROP INDEX IF EXISTS idx_income_statement_non_interest_expenses_gin")
        
        # Create income_statement table based on income-statement.sql structure
        create_table_query = """
        CREATE TABLE income_statement (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "interestIncome" JSONB,
            "interestIncomeValue" DECIMAL(18,2),
            "interestExpenses" JSONB,
            "interestExpensesValue" DECIMAL(18,2),
            "badDebtsWrittenOffNotProvided" DECIMAL(18,2),
            "provisionBadDoubtfulDebts" DECIMAL(18,2),
            "impairmentsInvestments" DECIMAL(18,2),
            "incomeTaxProvision" DECIMAL(18,2),
            "extraordinaryCreditsCharge" DECIMAL(18,2),
            "nonCoreCreditsCharges" JSONB,
            "nonCoreCreditsChargesValue" DECIMAL(18,2),
            "nonInterestIncome" JSONB,
            "nonInterestIncomeValue" DECIMAL(18,2),
            "nonInterestExpenses" JSONB,
            "nonInterestExpensesValue" DECIMAL(18,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_income_statement_reporting_date ON income_statement(\"reportingDate\")")
        cursor.execute("CREATE INDEX idx_income_statement_created_at ON income_statement(created_at)")
        cursor.execute("CREATE INDEX idx_income_statement_interest_income_gin ON income_statement USING GIN (\"interestIncome\")")
        cursor.execute("CREATE INDEX idx_income_statement_interest_expenses_gin ON income_statement USING GIN (\"interestExpenses\")")
        cursor.execute("CREATE INDEX idx_income_statement_non_core_credits_gin ON income_statement USING GIN (\"nonCoreCreditsCharges\")")
        cursor.execute("CREATE INDEX idx_income_statement_non_interest_income_gin ON income_statement USING GIN (\"nonInterestIncome\")")
        cursor.execute("CREATE INDEX idx_income_statement_non_interest_expenses_gin ON income_statement USING GIN (\"nonInterestExpenses\")")
        
        conn.commit()
        print("Income statement table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'income_statement' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\nIncome statement table created with {len(columns)} columns:")
        for col in columns:
            print(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating income statement table: {e}")
        raise


if __name__ == "__main__":
    create_income_statement_table()