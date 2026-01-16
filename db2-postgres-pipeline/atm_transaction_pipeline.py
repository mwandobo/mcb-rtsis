#!/usr/bin/env python3
"""
ATM Transaction Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.atm_transaction_processor import AtmTransactionProcessor

class AtmTransactionPipeline:
    def __init__(self, limit=None, start_date=None, resume=False):
        """
        ATM Transaction Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            start_date (str): Starting date for transactions (YYYY-MM-DD format)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get ATM transaction table config
        self.table_config = self.config.tables.get('atmTransaction')
        if not self.table_config:
            raise ValueError("ATM transaction table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.start_date = start_date or '2024-01-01'  # Default to 2024 data for ATM transactions
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.atm_transaction_processor = AtmTransactionProcessor()
        
        self.logger.info(f"ATM Transaction Pipeline initialized")
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
    
    def get_last_processed_record(self):
        """Get the last processed record from PostgreSQL to resume from"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT "transactionDate", "transactionId"
                    FROM "{self.table_config.target_table}"
                    ORDER BY "transactionDate" DESC, "transactionId" DESC
                    LIMIT 1;
                """)
                
                result = cursor.fetchone()
                if result:
                    return str(result[0]), str(result[1])
                return None, None
                
        except Exception as e:
            self.logger.error(f"Failed to get last processed record: {e}")
            return None, None
    
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
    
    def get_atm_transaction_query(self, last_processed_date=None, last_processed_id=None):
        """Get ATM transaction query with cursor-based pagination"""
        
        # Build WHERE clause for pagination and date filtering
        where_conditions = ["TERMINAL in('MWL01001','MWL01002')"]
        
        if self.start_date:
            where_conditions.append(f"atx.TUN_DATE >= DATE('{self.start_date}')")
        
        if last_processed_date and last_processed_id:
            where_conditions.append(f"""
            (atx.TUN_DATE > DATE('{last_processed_date}') 
             OR (atx.TUN_DATE = DATE('{last_processed_date}') AND atx.REFERENCE_NUMBER > '{last_processed_id}'))
            """)
        
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        return f"""
        select
            CURRENT_TIMESTAMP as reportingDate,
            atx.TERMINAL as atmCode,
            atx.TUN_DATE as transactionDate,
            atx.REFERENCE_NUMBER as transactionId,
            CASE
                WHEN atx.PROCESSING_CODE IN ('010000','011000','011096','012000') THEN 'Cash Withdrawal'
                WHEN atx.PROCESSING_CODE IN ('001000','002000','311000','312000') THEN 'Account balance enquiries'
                WHEN atx.PROCESSING_CODE = '219610' THEN 'Reversal/Cancellation'
                ELSE NULL
            END as transactionType,
            'TZS' as currency,
            atx.TRANSACTION_AMOUNT as orgTransactionAmount,
            atx.TRANSACTION_AMOUNT as tzsTransactionAmount,
            'Card and Mobile Based' as atmChannel,
            DECIMAL(atx.TRANSACTION_AMOUNT * 0.18, 15, 2) AS valueAddedTaxAmount,
            0 as exciseDutyAmount,
            0 as electronicLevyAmount
        FROM ATM_TRX_RECORDING atx
        LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = atx.PROCESSING_CODE 
        {where_clause}
        ORDER BY atx.TUN_DATE ASC, atx.REFERENCE_NUMBER ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
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
        """Get total count of ATM transaction records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = """
                SELECT COUNT(*) 
                FROM ATM_TRX_RECORDING atx
                LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = atx.PROCESSING_CODE 
                WHERE TERMINAL in('MWL01001','MWL01002')
                """
                
                if self.start_date:
                    count_query += f" AND atx.TUN_DATE >= DATE('{self.start_date}')"
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"Failed to get total count: {e}")
            return 0

    def run_complete_pipeline(self):
        """Run the complete ATM transaction pipeline"""
        self.logger.info("Starting Complete ATM Transaction Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data and resume point
            existing_count = self.get_existing_count()
            self.logger.info(f"Existing records in PostgreSQL: {existing_count}")
            
            last_processed_date = None
            last_processed_id = None
            
            if self.resume and existing_count > 0:
                last_processed_date, last_processed_id = self.get_last_processed_record()
                self.logger.info(f"Resuming from: Date={last_processed_date}, ID={last_processed_id}")
            elif not self.resume and existing_count > 0:
                self.logger.warning("Existing data found but resume=False. Use resume=True to continue from last record.")
            
            # Step 2: Get total count
            self.logger.info(f"Getting total ATM transaction records from {self.start_date}...")
            total_records = self.get_total_count()
            self.logger.info(f"Total ATM transaction records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("No ATM transaction records found")
                return
            
            # Step 3: Process in batches using cursor-based pagination
            total_processed = existing_count  # Start count from existing records
            batch_number = 1
            
            while True:
                self.logger.info(f"\nBatch {batch_number}: Fetching next batch of records...")
                
                # Fetch batch
                with self.get_db2_connection() as conn:
                    cursor = conn.cursor()
                    
                    atm_query = self.get_atm_transaction_query(last_processed_date, last_processed_id)
                    self.logger.info("Executing ATM transaction query...")
                    
                    cursor.execute(atm_query)
                    rows = cursor.fetchall()
                    
                    self.logger.info(f"Fetched {len(rows)} ATM transaction records in batch {batch_number}")
                    
                    if not rows:
                        self.logger.info("No more records found - processing complete!")
                        break
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("Sample ATM transaction records:")
                        for i, row in enumerate(rows[:min(3, len(rows))], 1):
                            self.logger.info(f"  {i}. ATM: {row[1]} | Date: {row[2]} | ID: {row[3]} | Type: {row[4]} | Amount: {row[6]}")
                
                # Process batch
                batch_processed = 0
                batch_skipped = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    for row in rows:
                        try:
                            # Process the record using the processor
                            record = self.atm_transaction_processor.process_record(row, self.table_config.name)
                            
                            if self.atm_transaction_processor.validate_record(record):
                                # Use upsert which handles duplicates automatically
                                self.atm_transaction_processor.insert_to_postgres(record, pg_cursor)
                                batch_processed += 1
                                
                                if batch_processed % 100 == 0:
                                    self.logger.info(f"Processed {batch_processed} records in batch {batch_number}...")
                            else:
                                self.logger.warning(f"Invalid ATM transaction record skipped: {record.transaction_id}")
                                batch_skipped += 1
                                
                        except Exception as e:
                            self.logger.error(f"Error processing ATM transaction record: {e}")
                            batch_skipped += 1
                            continue
                    
                    # Commit the entire batch at once
                    try:
                        conn.commit()
                        self.logger.info(f"Batch {batch_number} committed successfully")
                    except Exception as e:
                        self.logger.error(f"Failed to commit batch {batch_number}: {e}")
                        conn.rollback()
                        continue
                    
                    # Update pagination cursors from last row
                    if rows:
                        last_row = rows[-1]
                        last_processed_date = str(last_row[2])  # transactionDate is at index 2
                        last_processed_id = str(last_row[3])    # transactionId is at index 3
                
                total_processed += batch_processed
                self.logger.info(f"Batch {batch_number} completed: {batch_processed} new records, {batch_skipped} duplicates/skipped")
                self.logger.info(f"Total processed so far: {total_processed}")
                self.logger.info(f"Last processed: {last_processed_date} | ID: {last_processed_id}")
                
                # Move to next batch
                batch_number += 1
                
                # Small delay between batches
                import time
                time.sleep(1)
            
            # Step 4: Final verification
            self.logger.info(f"\nATM TRANSACTION PIPELINE FINISHED!")
            self.logger.info(f"Total records processed: {total_processed}")
            self.logger.info(f"Total batches: {batch_number - 1}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data (using transactionNature as that's the column name in DB)
                try:
                    cursor.execute(f"""
                        SELECT "atmCode", "transactionDate", "transactionType", "orgTransactionAmount", "currency"
                        FROM "{self.table_config.target_table}" 
                        ORDER BY "transactionDate" ASC
                        LIMIT 5;
                    """)
                except Exception:
                    # Fallback if column name is different
                    cursor.execute(f"""
                        SELECT "atmCode", "transactionDate", "orgTransactionAmount", "currency"
                        FROM "{self.table_config.target_table}" 
                        ORDER BY "transactionDate" ASC
                        LIMIT 5;
                    """)
                
                sample_records = cursor.fetchall()
                self.logger.info("Sample of processed records:")
                for record in sample_records:
                    atm_code, transaction_date, transaction_type, amount, currency = record
                    self.logger.info(f"  {atm_code} | {transaction_date} | {transaction_type} | {amount} {currency}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("Complete ATM Transaction Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for ATM transaction processing - starting from 2024
    pipeline = AtmTransactionPipeline(start_date='2024-01-01', resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()