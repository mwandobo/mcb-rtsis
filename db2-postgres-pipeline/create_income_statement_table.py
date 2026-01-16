#!/usr/bin/env python3
"""
Create Income Statement Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_income_statement_table():
    """Create the income statement table in PostgreSQL"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        logger.info("🗑️ Dropping existing incomeStatement table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "incomeStatement" CASCADE;')
        
        # Create Income Statement table
        logger.info("🏗️ Creating incomeStatement table...")
        create_table_sql = """
        CREATE TABLE "incomeStatement" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20),
            "interestIncome" JSONB,
            "interestIncomeValue" DECIMAL(31,2),
            "interestExpenses" JSONB,
            "interestExpensesValue" DECIMAL(31,2),
            "badDebtsWrittenOffNotProvided" DECIMAL(31,2),
            "provisionBadDoubtfulDebts" DECIMAL(31,2),
            "impairmentsInvestments" DECIMAL(31,2),
            "incomeTaxProvision" DECIMAL(31,2),
            "extraordinaryCreditsCharge" DECIMAL(31,2),
            "nonCoreCreditsCharges" JSONB,
            "nonCoreCreditsChargesValue" DECIMAL(31,2),
            "nonInterestIncome" JSONB,
            "nonInterestIncomeValue" DECIMAL(31,2),
            "nonInterestExpenses" JSONB,
            "nonInterestExpensesValue" DECIMAL(31,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_income_statement_reporting_date ON "incomeStatement"("reportingDate");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_interest_income_value ON "incomeStatement"("interestIncomeValue");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_interest_expenses_value ON "incomeStatement"("interestExpensesValue");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_non_interest_income_value ON "incomeStatement"("nonInterestIncomeValue");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_non_interest_expenses_value ON "incomeStatement"("nonInterestExpensesValue");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_interest_income_gin ON "incomeStatement" USING GIN ("interestIncome");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_interest_expenses_gin ON "incomeStatement" USING GIN ("interestExpenses");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_non_interest_income_gin ON "incomeStatement" USING GIN ("nonInterestIncome");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_non_interest_expenses_gin ON "incomeStatement" USING GIN ("nonInterestExpenses");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_non_core_credits_gin ON "incomeStatement" USING GIN ("nonCoreCreditsCharges");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Income statement table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, numeric_precision, numeric_scale 
            FROM information_schema.columns 
            WHERE table_name = 'incomeStatement' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for col_name, data_type, precision, scale in columns:
            precision_info = f"({precision},{scale})" if precision and scale else ""
            logger.info(f"  {col_name}: {data_type}{precision_info}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to create income statement table: {e}")
        raise

if __name__ == "__main__":
    create_income_statement_table()