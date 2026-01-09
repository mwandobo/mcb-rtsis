#!/usr/bin/env python3
"""
Income Statement Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.income_statement_processor import IncomeStatementProcessor

class IncomeStatementPipeline:
    def __init__(self, limit=None, start_date=None, resume=False):
        """
        Income Statement Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            start_date (str): Starting date for transactions (YYYY-MM-DD format)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get income statement table config
        self.table_config = self.config.tables.get('incomeStatement')
        if not self.table_config:
            raise ValueError("Income statement table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.start_date = start_date or '2024-01-01'  # Default to 2024 data
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.income_statement_processor = IncomeStatementProcessor()
        
        self.logger.info(f"Income Statement Pipeline initialized")
        self.logger.info(f"Batch limit: {self.limit}")
        self.logger.info(f"Start date: {self.start_date}")
        self.logger.info(f"Resume mode: {self.resume}")
        
    @contextmanager
    def get_db2_connection(self):
        """Get DB2 connection"""
        with self.db2_conn.get_connection() as conn:
            yield conn
            
    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            yield conn
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_existing_count(self):
        """Get count of existing records in PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                # Check if we already have a record for today
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}" WHERE DATE("reportingDate") = CURRENT_DATE;')
                today_count = cursor.fetchone()[0]
                if today_count > 0:
                    self.logger.info(f"Found {today_count} existing income statement record(s) for today")
                    return today_count
                
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Failed to get existing count: {e}")
            return 0
    
    def get_total_count(self):
        """Get total count of income statement records (should be 1 aggregated record)"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Count query - should return 1 for aggregated income statement
                count_query = """
                SELECT COUNT(*) FROM (
                    SELECT 1
                    FROM GLI_TRX_EXTRACT gte
                    LEFT JOIN GLG_ACCOUNT gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
                    WHERE (gl.EXTERNAL_GLACCOUNT LIKE '4%' 
                       OR gl.EXTERNAL_GLACCOUNT LIKE '5%'
                       OR gl.EXTERNAL_GLACCOUNT LIKE '6%'
                       OR gl.EXTERNAL_GLACCOUNT LIKE '7%')
                ) AS income_data
                """
                
                if self.start_date:
                    count_query = count_query.replace(
                        ") AS income_data",
                        f" AND gte.TRN_DATE >= DATE('{self.start_date}')) AS income_data"
                    )
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return 1 if total > 0 else 0  # Income statement is always 1 aggregated record
                
        except Exception as e:
            self.logger.error(f"Failed to get total count: {e}")
            return 0

    def run_complete_pipeline(self):
        """Run the complete income statement pipeline"""
        self.logger.info("Starting Complete Income Statement Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data
            existing_count = self.get_existing_count()
            self.logger.info(f"Existing records in PostgreSQL: {existing_count}")
            
            # Skip if we already have today's income statement
            if existing_count > 0:
                self.logger.info("Income statement for today already exists. Skipping to avoid duplicates.")
                self.logger.info("Use clean_income_statement.py to clear existing records if you want to regenerate.")
                return
            
            # Step 2: Get total count
            self.logger.info(f"Getting income statement data from {self.start_date}...")
            total_records = self.get_total_count()
            self.logger.info(f"Income statement data available: {total_records} (aggregated record)")
            
            if total_records == 0:
                self.logger.info("No income statement data found")
                return
            
            # Step 3: Process using the SQL query from config
            self.logger.info("Executing income statement query...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the query from config with date filter
                query = self.table_config.query
                if self.start_date:
                    # Add date filter to the existing query
                    query = query.replace(
                        "WHERE gl.EXTERNAL_GLACCOUNT LIKE '4%'",
                        f"WHERE gte.TRN_DATE >= DATE('{self.start_date}') AND gl.EXTERNAL_GLACCOUNT LIKE '4%'"
                    )
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                self.logger.info(f"Fetched {len(rows)} income statement record(s)")
                
                if not rows:
                    self.logger.info("No records found matching criteria")
                    return
                
                # Show sample data
                self.logger.info("Income statement data:")
                for i, row in enumerate(rows, 1):
                    self.logger.info(f"  {i}. Interest Income: {row[1]:,.2f} | Interest Expense: {row[2]:,.2f}")
                    self.logger.info(f"     Non-Interest Income: {row[6]:,.2f} | Non-Interest Expenses: {row[7]:,.2f}")
            
            # Step 4: Process records
            processed_count = 0
            skipped_count = 0
            
            with self.get_postgres_connection() as conn:
                pg_cursor = conn.cursor()
                
                for i, row in enumerate(rows, 1):
                    try:
                        # Process the record using the processor
                        record = self.income_statement_processor.process_record(row, self.table_config.name)
                        
                        if self.income_statement_processor.validate_record(record):
                            # Use upsert to replace existing income statement
                            self.income_statement_processor.upsert_to_postgres(record, pg_cursor)
                            processed_count += 1
                            
                            self.logger.info(f"Processed income statement record {processed_count}")
                        else:
                            self.logger.warning(f"Invalid income statement record skipped")
                            skipped_count += 1
                            
                    except Exception as e:
                        self.logger.error(f"Error processing record {i}: {e}")
                        skipped_count += 1
                        continue
                
                # Commit all changes
                try:
                    conn.commit()
                    self.logger.info(f"All records committed successfully")
                except Exception as e:
                    self.logger.error(f"Failed to commit records: {e}")
                    conn.rollback()
                    return
            
            # Step 5: Final verification
            self.logger.info(f"INCOME STATEMENT PIPELINE FINISHED!")
            self.logger.info(f"Total records processed: {processed_count}")
            self.logger.info(f"Records skipped: {skipped_count}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data
                cursor.execute(f"""
                    SELECT "reportingDate", "interestIncome", "interestExpense", "nonInterestIncome", "nonInterestExpenses"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "reportingDate" DESC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("Sample of processed records:")
                for record in sample_records:
                    reporting_date, interest_income, interest_expense, non_interest_income, non_interest_expenses = record
                    net_interest = (interest_income or 0) - (interest_expense or 0)
                    self.logger.info(f"  Date: {reporting_date} | Net Interest: {net_interest:,.2f} | Non-Interest Income: {non_interest_income:,.2f}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("Complete Income Statement Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for income statement processing - starting from 2024
    pipeline = IncomeStatementPipeline(start_date='2024-01-01', resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()