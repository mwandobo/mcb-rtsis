#!/usr/bin/env python3
"""
Card Transaction Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.card_transaction_processor import CardTransactionProcessor

class CardTransactionPipeline:
    def __init__(self, limit=None, start_date=None, resume=False):
        """
        Card Transaction Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            start_date (str): Starting date for transactions (YYYY-MM-DD format)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get card transaction table config
        self.table_config = self.config.tables.get('card_transaction')
        if not self.table_config:
            raise ValueError("Card transaction table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.start_date = start_date or '2024-01-01'  # Default to 2024 data
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.card_transaction_processor = CardTransactionProcessor()
        
        self.logger.info(f"💳 Card Transaction Pipeline initialized")
        self.logger.info(f"📊 Batch limit: {self.limit}")
        self.logger.info(f"📅 Start date: {self.start_date}")
        self.logger.info(f"🔄 Resume mode: {self.resume}")
        
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
            self.logger.error(f"❌ Failed to get last processed record: {e}")
            return None, None
    
    def get_existing_count(self):
        """Get count of existing records in PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"❌ Failed to get existing count: {e}")
            return 0
    
    def get_card_transaction_query(self, last_processed_date=None, last_processed_ref=None):
        """Get card transaction query with cursor-based pagination"""
        
        # Build WHERE clause for pagination and date filtering
        where_conditions = [f"ce.TUN_DATE >= DATE('{self.start_date}')"]
        
        if last_processed_date and last_processed_ref:
            where_conditions.append(f"""
            (ce.TUN_DATE > DATE('{last_processed_date}') 
             OR (ce.TUN_DATE = DATE('{last_processed_date}') AND ce.ISO_REF_NUM > '{last_processed_ref}'))
            """)
        
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Build the complete query with pagination (using new query from card_transaction.sql)
        return f"""
        SELECT
            CURRENT_TIMESTAMP AS reportingDate,
            ca.FULL_CARD_NO as cardNumber,
            LPAD(CHAR(ca.CARD_NUMBER), 10, '0') AS binNumber,
            'Mwalimu Commercial Bank Plc' as transactingBankName,
            ce.ISO_REF_NUM as transactionId,
            ce.TUN_DATE as transactionDate,
            'Local Transactions by Locally Issued Cards' as transactionNature,
            null as atmCode,
            null as posNumber,
            pc.DESCRIPTION as transactionDescription,
            ca.CARD_NAME_LATIN as beneficiaryName,
            null as beneficiaryTradeName,
            'TANZANIA, UNITED REPUBLIC OF' as beneficaryCountry,
            'TANZANIA, UNITED REPUBLIC OF' as transactionPlace,
            null as qtyItemsPurchased,
            null as unitPrice,
            null as orgFacilitatorCommissionAmount,
            null as usdFacilitatorCommissionAmount,
            null as tzsFacilitatorCommissionAmount,
            'TZS' as currency,
            ce.TRANSACTION_AMNT as orgTransactionAmount,
            CAST(ROUND(ce.TRANSACTION_AMNT / 2500.0, 2) AS DECIMAL(15,2)) AS usdTransactionAmount,
            ce.TRANSACTION_AMNT as tzsTransactionAmount
        FROM CMS_CARD_EXTRAIT ce
        LEFT JOIN CMS_CARD ca ON ca.CARD_SN = ce.CARD_SN
        LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = ce.PROCESS_CD
        {where_clause}
        ORDER BY ce.TUN_DATE ASC, ce.ISO_REF_NUM ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
    
    def get_total_count(self):
        """Get total count of card transaction records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = f"""
                SELECT COUNT(*) 
                FROM CMS_CARD_EXTRAIT ce
                WHERE ce.TUN_DATE >= DATE('{self.start_date}')
                """
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def run_complete_pipeline(self):
        """Run the complete card transaction pipeline"""
        self.logger.info("🚀 Starting Complete Card Transaction Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data and resume point
            existing_count = self.get_existing_count()
            self.logger.info(f"📊 Existing records in PostgreSQL: {existing_count}")
            
            last_processed_date = None
            last_processed_ref = None
            
            if self.resume and existing_count > 0:
                last_processed_date, last_processed_ref = self.get_last_processed_record()
                self.logger.info(f"🔄 Resuming from: Date={last_processed_date}, Ref={last_processed_ref}")
            elif not self.resume and existing_count > 0:
                self.logger.warning("⚠️ Existing data found but resume=False. Use resume=True to continue from last record.")
            
            # Step 2: Get total count
            self.logger.info(f"📊 Getting total card transaction records from {self.start_date}...")
            total_records = self.get_total_count()
            self.logger.info(f"📊 Total card transaction records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("ℹ️ No card transaction records found")
                return
            
            # Step 3: Process in batches using cursor-based pagination
            total_processed = existing_count  # Start count from existing records
            batch_number = 1
            
            while True:
                self.logger.info(f"\n📊 Batch {batch_number}: Fetching next batch of records...")
                
                # Fetch batch
                with self.get_db2_connection() as conn:
                    cursor = conn.cursor()
                    
                    transaction_query = self.get_card_transaction_query(last_processed_date, last_processed_ref)
                    self.logger.info("📊 Executing card transaction query...")
                    
                    cursor.execute(transaction_query)
                    rows = cursor.fetchall()
                    
                    self.logger.info(f"💳 Fetched {len(rows)} card transaction records in batch {batch_number}")
                    
                    if not rows:
                        self.logger.info("ℹ️ No more records found - processing complete!")
                        break
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample card transaction records:")
                        for i, row in enumerate(rows[:min(3, len(rows))], 1):
                            # Mask card number for security
                            card_number = str(row[1])
                            masked_card = card_number[:4] + '****' + card_number[-4:] if len(card_number) >= 8 else card_number
                            self.logger.info(f"  {i}. Card: {masked_card} | Transaction: {row[4]} | Nature: {row[6]} | Amount: {row[20]}")
                
                # Process batch
                batch_processed = 0
                batch_skipped = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    for row in rows:
                        try:
                            # Process the record using the processor
                            record = self.card_transaction_processor.process_record(row, self.table_config.name)
                            
                            if self.card_transaction_processor.validate_record(record):
                                # Use upsert which handles duplicates automatically
                                self.card_transaction_processor.insert_to_postgres(record, pg_cursor)
                                batch_processed += 1
                                
                                if batch_processed % 100 == 0:
                                    self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                            else:
                                self.logger.warning(f"⚠️ Invalid card transaction record skipped: {record.transaction_id}")
                                batch_skipped += 1
                                
                        except Exception as e:
                            self.logger.error(f"❌ Error processing card transaction record: {e}")
                            batch_skipped += 1
                            continue
                    
                    # Commit the entire batch at once
                    try:
                        conn.commit()
                        self.logger.info(f"✅ Batch {batch_number} committed successfully")
                    except Exception as e:
                        self.logger.error(f"❌ Failed to commit batch {batch_number}: {e}")
                        conn.rollback()
                        continue
                    
                    # Update pagination cursors from last row
                    if rows:
                        last_row = rows[-1]
                        last_processed_date = str(last_row[5])  # transactionDate is at index 5
                        last_processed_ref = str(last_row[4])   # transactionId is at index 4
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} new records, {batch_skipped} duplicates/skipped")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
                self.logger.info(f"📅 Last processed: {last_processed_date} | Ref: {last_processed_ref}")
                
                # Move to next batch
                batch_number += 1
                
                # Small delay between batches
                import time
                time.sleep(1)
            
            # Step 4: Final verification
            self.logger.info(f"\n🎉 COMPLETE CARD TRANSACTION PIPELINE FINISHED!")
            self.logger.info(f"📊 Total records processed: {total_processed}")
            self.logger.info(f"📊 Total batches: {batch_number - 1}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"📊 Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data
                cursor.execute(f"""
                    SELECT "cardNumber", "transactionId", "transactionNature", "orgTransactionAmount", "transactionDate"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "transactionDate" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("📋 Sample of processed records:")
                for record in sample_records:
                    card_number, transaction_id, transaction_nature, amount, transaction_date = record
                    # Mask card number for security
                    masked_card = card_number[:4] + '****' + card_number[-4:] if len(card_number) >= 8 else card_number
                    self.logger.info(f"  💳 {masked_card} | {transaction_id} | {transaction_nature} | {amount} | {transaction_date}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("💳 Complete Card Transaction Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for transaction processing
    pipeline = CardTransactionPipeline(start_date='2024-01-01', resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()