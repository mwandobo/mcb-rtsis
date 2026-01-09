#!/usr/bin/env python3
"""
Balances with BOT Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.bot_balances_processor import BotBalancesProcessor

class BalancesBotPipeline:
    def __init__(self, limit=None, start_date=None, resume=False):
        """
        Balances with BOT Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            start_date (str): Starting date for transactions (YYYY-MM-DD format)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get balances bot table config
        self.table_config = self.config.tables.get('balances_bot')
        if not self.table_config:
            raise ValueError("Balances bot table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.start_date = start_date or '2024-01-01'  # Default to 2024 data
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.bot_balances_processor = BotBalancesProcessor()
        
        self.logger.info(f"Balances with BOT Pipeline initialized")
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
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Failed to get existing count: {e}")
            return 0
    
    def get_total_count(self):
        """Get total count of balances with BOT records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = """
                SELECT COUNT(*) 
                FROM GLI_TRX_EXTRACT AS gte
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                JOIN CUSTOMER c ON c.CUST_ID = gte.CUST_ID
                LEFT JOIN CURRENCY cu ON UPPER(TRIM(cu.SHORT_DESCR)) = UPPER(TRIM(gte.CURRENCY_SHORT_DES))
                WHERE gl.EXTERNAL_GLACCOUNT='100028000'
                """
                
                if self.start_date:
                    count_query += f" AND gte.TRN_DATE >= DATE('{self.start_date}')"
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"Failed to get total count: {e}")
            return 0

    def run_complete_pipeline(self):
        """Run the complete balances with BOT pipeline"""
        self.logger.info("Starting Complete Balances with BOT Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data
            existing_count = self.get_existing_count()
            self.logger.info(f"Existing records in PostgreSQL: {existing_count}")
            
            # Step 2: Get total count
            self.logger.info(f"Getting total balances with BOT records from {self.start_date}...")
            total_records = self.get_total_count()
            self.logger.info(f"Total balances with BOT records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("No balances with BOT records found")
                return
            
            # Step 3: Process using the SQL query from config
            self.logger.info("Executing balances with BOT query...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the query from config with date filter and remove FETCH FIRST limitation
                query = self.table_config.query
                if self.start_date:
                    # Add date filter to the existing WHERE clause
                    if "WHERE" in query:
                        query = query.replace(";", f" AND gte.TRN_DATE >= DATE('{self.start_date}');")
                    else:
                        query = query.replace(";", f" WHERE gte.TRN_DATE >= DATE('{self.start_date}');")
                
                # Remove FETCH FIRST limitation to get all records (limit to 100 for testing)
                query = query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 100 ROWS ONLY")
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                self.logger.info(f"Fetched {len(rows)} balances with BOT records")
                
                if not rows:
                    self.logger.info("No records found matching criteria")
                    return
                
                # Show sample data
                self.logger.info("Sample balances with BOT records:")
                for i, row in enumerate(rows[:min(3, len(rows))], 1):
                    self.logger.info(f"  {i}. Account: {row[1]} | Bank: {row[2]} | Currency: {row[5]} | Amount: {row[6]}")
            
            # Step 4: Process records
            processed_count = 0
            skipped_count = 0
            
            with self.get_postgres_connection() as conn:
                pg_cursor = conn.cursor()
                
                for i, row in enumerate(rows, 1):
                    try:
                        # Process the record using the processor
                        record = self.bot_balances_processor.process_record(row, self.table_config.name)
                        
                        if self.bot_balances_processor.validate_record(record):
                            # Use upsert which handles duplicates automatically
                            self.bot_balances_processor.insert_to_postgres(record, pg_cursor)
                            processed_count += 1
                            
                            if processed_count % 100 == 0:
                                self.logger.info(f"Processed {processed_count}/{len(rows)} records...")
                        else:
                            self.logger.warning(f"Invalid balances with BOT record skipped: {record.account_number}")
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
            self.logger.info(f"BALANCES WITH BOT PIPELINE FINISHED!")
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
                    SELECT "accountNumber", "accountName", "accountType", "currency", "orgAmount", "transactionDate"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "transactionDate" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("Sample of processed records:")
                for record in sample_records:
                    account_number, account_name, account_type, currency, amount, transaction_date = record
                    self.logger.info(f"  {account_number} | {account_name} | {account_type} | {currency} | {amount} | {transaction_date}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("Complete Balances with BOT Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for balance processing - starting from January 2024
    pipeline = BalancesBotPipeline(start_date='2024-01-01', resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()