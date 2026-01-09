#!/usr/bin/env python3
"""
Banker Cheques and Drafts Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.banker_cheques_drafts_processor import BankerChequesDraftsProcessor

class BankerChequesDraftsPipeline:
    def __init__(self, limit=None, start_date=None, resume=False):
        """
        Banker Cheques and Drafts Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            start_date (str): Starting date for transactions (YYYY-MM-DD format)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get banker cheques and drafts table config
        self.table_config = self.config.tables.get('bankerChequesDrafts')
        if not self.table_config:
            raise ValueError("Banker cheques and drafts table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.start_date = start_date or '2024-01-01'  # Default to 2024 data
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.banker_cheques_drafts_processor = BankerChequesDraftsProcessor()
        
        self.logger.info(f"Banker Cheques and Drafts Pipeline initialized")
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
        """Get total count of banker cheques and drafts records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = """
                WITH pa_unique AS (
                    SELECT DEP_ACC_NUMBER,
                           CUST_ID,
                           LIMIT_CURRENCY,
                           ACCOUNT_NUMBER,
                           ROW_NUMBER() OVER (PARTITION BY DEP_ACC_NUMBER ORDER BY ACCOUNT_NUMBER) AS rn
                    FROM PROFITS_ACCOUNT
                )
                SELECT COUNT(*) 
                FROM (
                    SELECT cbi.*,
                           pa.CUST_ID,
                           pa.LIMIT_CURRENCY
                    FROM CHEQUE_BOOK_ITEM cbi
                    LEFT JOIN pa_unique pa
                           ON pa.DEP_ACC_NUMBER = cbi.ACCOUNT_NUMBER
                          AND pa.rn = 1
                ) nr
                LEFT JOIN PROFITS.W_DIM_CUSTOMER cu
                       ON cu.CUST_ID = nr.CUST_ID
                LEFT JOIN CURRENCY c
                       ON c.ID_CURRENCY = nr.LIMIT_CURRENCY
                WHERE nr.CHEQUE_AMOUNT > 0
                """
                
                if self.start_date:
                    count_query += f" AND nr.TRX_DATE >= DATE('{self.start_date}')"
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"Failed to get total count: {e}")
            return 0

    def run_complete_pipeline(self):
        """Run the complete banker cheques and drafts pipeline"""
        self.logger.info("Starting Complete Banker Cheques and Drafts Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data
            existing_count = self.get_existing_count()
            self.logger.info(f"Existing records in PostgreSQL: {existing_count}")
            
            # Step 2: Get total count
            self.logger.info(f"Getting total banker cheques and drafts records from {self.start_date}...")
            total_records = self.get_total_count()
            self.logger.info(f"Total banker cheques and drafts records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("No banker cheques and drafts records found")
                return
            
            # Step 3: Process using the SQL query from config
            self.logger.info("Executing banker cheques and drafts query...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the query from config with date filter
                query = self.table_config.query
                if self.start_date:
                    # Replace the existing WHERE clause to add date filter
                    query = query.replace(
                        "WHERE nr.CHEQUE_AMOUNT > 0",
                        f"WHERE nr.CHEQUE_AMOUNT > 0 AND nr.TRX_DATE >= DATE('{self.start_date}')"
                    )
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                self.logger.info(f"Fetched {len(rows)} banker cheques and drafts records")
                
                if not rows:
                    self.logger.info("No records found matching criteria")
                    return
                
                # Show sample data
                self.logger.info("Sample banker cheques and drafts records:")
                for i, row in enumerate(rows[:min(3, len(rows))], 1):
                    self.logger.info(f"  {i}. Customer: {row[1]} | Name: {row[2]} | Amount: {row[9]} {row[8]}")
            
            # Step 4: Process records
            processed_count = 0
            skipped_count = 0
            
            with self.get_postgres_connection() as conn:
                pg_cursor = conn.cursor()
                
                for i, row in enumerate(rows, 1):
                    try:
                        # Process the record using the processor
                        record = self.banker_cheques_drafts_processor.process_record(row, self.table_config.name)
                        
                        if self.banker_cheques_drafts_processor.validate_record(record):
                            # Use regular insert to keep all transaction records
                            self.banker_cheques_drafts_processor.insert_to_postgres(record, pg_cursor)
                            processed_count += 1
                            
                            if processed_count % 100 == 0:
                                self.logger.info(f"Processed {processed_count}/{len(rows)} records...")
                        else:
                            self.logger.warning(f"Invalid banker cheques and drafts record skipped: {record.customer_identification_number}")
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
            self.logger.info(f"BANKER CHEQUES AND DRAFTS PIPELINE FINISHED!")
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
                    SELECT "customerIdentificationNumber", "customerName", "currency", "orgAmount", "transactionDate"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "transactionDate" DESC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("Sample of processed records:")
                for record in sample_records:
                    customer_id, customer_name, currency, amount, transaction_date = record
                    self.logger.info(f"  {customer_id} | {customer_name} | {amount} {currency} | {transaction_date}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("Complete Banker Cheques and Drafts Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for banker cheques and drafts processing - starting from 2024
    pipeline = BankerChequesDraftsPipeline(limit=100, start_date='2024-01-01', resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()