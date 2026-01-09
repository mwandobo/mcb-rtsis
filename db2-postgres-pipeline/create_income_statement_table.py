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
            "reportingDate" TIMESTAMP,
            "interestIncome" DECIMAL(31,2),
            "interestExpense" DECIMAL(31,2),
            "badDebtsWrittenOffNotProvided" DECIMAL(31,2),
            "provisionBadDoubtfulDebts" DECIMAL(31,2),
            "impairmentsInvestments" DECIMAL(31,2),
            "nonInterestIncome" DECIMAL(31,2),
            "nonInterestExpenses" DECIMAL(31,2),
            "incomeTaxProvision" DECIMAL(31,2),
            "extraordinaryCreditsCharge" DECIMAL(31,2),
            "nonCoreCreditsCharges" DECIMAL(31,2),
            "amountInterestIncome" DECIMAL(31,2),
            "amountInterestExpenses" DECIMAL(31,2),
            "amountNonInterestIncome" DECIMAL(31,2),
            "amountNonInterestExpenses" DECIMAL(31,2),
            "amountnonCoreCreditsCharges" DECIMAL(31,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_income_statement_reporting_date ON "incomeStatement"("reportingDate");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_interest_income ON "incomeStatement"("interestIncome");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_interest_expense ON "incomeStatement"("interestExpense");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_non_interest_income ON "incomeStatement"("nonInterestIncome");',
            'CREATE INDEX IF NOT EXISTS idx_income_statement_non_interest_expenses ON "incomeStatement"("nonInterestExpenses");'
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