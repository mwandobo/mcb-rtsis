#!/usr/bin/env python3
"""
Digital Credit Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.digital_credit_processor import DigitalCreditProcessor

class DigitalCreditPipeline:
    def __init__(self, limit=None, start_date=None, resume=False):
        """
        Digital Credit Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            start_date (str): Starting date for loans (YYYY-MM-DD format)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get digital credit table config
        self.table_config = self.config.tables.get('digitalCredit')
        if not self.table_config:
            raise ValueError("Digital credit table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.start_date = start_date or '2020-01-01'  # Default to 2020 data for digital credit
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.digital_credit_processor = DigitalCreditProcessor()
        
        self.logger.info(f"Digital Credit Pipeline initialized")
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
        """Get total count of digital credit records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = """
                SELECT COUNT(*) 
                FROM CUSTOMER c
                INNER JOIN LOAN_ACCOUNT la ON c.CUST_ID = la.CUST_ID
                LEFT JOIN PRODUCT p ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT
                LEFT JOIN BANK_PARAMETERS bp ON 1=1
                WHERE 
                    la.LOAN_STATUS IS NOT NULL
                    AND la.ACC_OPEN_DT IS NOT NULL
                    AND la.ACC_OPEN_DT >= '2018-01-01'
                    AND (
                        c.MOBILE_TEL IS NOT NULL
                        OR la.TOT_DRAWDOWN_AMN <= 10000000
                        OR la.INSTALL_COUNT <= 60
                        OR la.ACC_OPEN_DT >= '2020-01-01'
                    )
                """
                
                if self.start_date:
                    count_query += f" AND la.ACC_OPEN_DT >= DATE('{self.start_date}')"
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"Failed to get total count: {e}")
            return 0

    def run_complete_pipeline(self):
        """Run the complete digital credit pipeline"""
        self.logger.info("Starting Complete Digital Credit Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data
            existing_count = self.get_existing_count()
            self.logger.info(f"Existing records in PostgreSQL: {existing_count}")
            
            # Step 2: Get total count
            self.logger.info(f"Getting total digital credit records from {self.start_date}...")
            total_records = self.get_total_count()
            self.logger.info(f"Total digital credit records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("No digital credit records found")
                return
            
            # Step 3: Process using the SQL query from config
            self.logger.info("Executing digital credit query...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the query from config with date filter and remove FETCH FIRST limitation
                query = self.table_config.query
                if self.start_date:
                    # Add date filter to the existing WHERE clause
                    if "WHERE" in query:
                        query = query.replace("ORDER BY", f" AND la.ACC_OPEN_DT >= DATE('{self.start_date}') ORDER BY")
                    else:
                        query = query.replace("ORDER BY", f" WHERE la.ACC_OPEN_DT >= DATE('{self.start_date}') ORDER BY")
                
                # Remove FETCH FIRST limitation to get all records (limit to 100 for testing)
                query = query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 100 ROWS ONLY")
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                self.logger.info(f"Fetched {len(rows)} digital credit records")
                
                if not rows:
                    self.logger.info("No records found matching criteria")
                    return
                
                # Show sample data
                self.logger.info("Sample digital credit records:")
                for i, row in enumerate(rows[:min(3, len(rows))], 1):
                    self.logger.info(f"  {i}. Customer: {row[1]} | Facilitator: {row[7]} | Product: {row[8]} | Amount: {row[9]}")
            
            # Step 4: Process records
            processed_count = 0
            skipped_count = 0
            
            with self.get_postgres_connection() as conn:
                pg_cursor = conn.cursor()
                
                for i, row in enumerate(rows, 1):
                    try:
                        # Process the record using the processor
                        record = self.digital_credit_processor.process_record(row, self.table_config.name)
                        
                        if self.digital_credit_processor.validate_record(record):
                            # Use upsert which handles duplicates automatically
                            self.digital_credit_processor.insert_to_postgres(record, pg_cursor)
                            processed_count += 1
                            
                            if processed_count % 100 == 0:
                                self.logger.info(f"Processed {processed_count}/{len(rows)} records...")
                        else:
                            self.logger.warning(f"Invalid digital credit record skipped: {record.loan_id}")
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
            self.logger.info(f"DIGITAL CREDIT PIPELINE FINISHED!")
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
                    SELECT "customerName", "servicesFacilitator", "productName", "tzsLoanBalance", "loanDisbursementDate"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "loanDisbursementDate" DESC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("Sample of processed records:")
                for record in sample_records:
                    customer_name, facilitator, product_name, balance, disbursement_date = record
                    self.logger.info(f"  {customer_name} | {facilitator} | {product_name} | {balance} | {disbursement_date}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("Complete Digital Credit Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for digital credit processing - starting from 2020
    pipeline = DigitalCreditPipeline(start_date='2020-01-01', resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()