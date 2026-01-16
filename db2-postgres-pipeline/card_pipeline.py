#!/usr/bin/env python3
"""
Card Information Pipeline Starting from 2016 - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.card_processor import CardProcessor

class CardPipeline2016:
    def __init__(self, start_year=2016, limit=10000):
        """
        Card Pipeline starting from specified year
        
        Args:
            start_year (int): Starting year for data extraction
            limit (int): Number of records to fetch per batch
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.start_year = start_year
        self.limit = limit
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.card_processor = CardProcessor()
        
        self.logger.info(f"💳 Card Pipeline initialized - Starting from {start_year}")
        self.logger.info(f"📊 Batch limit: {limit}")
        
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
    
    def clear_existing_data(self):
        """Clear existing card data from PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM "cardInformation";')
                conn.commit()
                self.logger.info("🗑️ Cleared existing card data from PostgreSQL")
        except Exception as e:
            self.logger.error(f"❌ Failed to clear existing data: {e}")
            raise
    
    def get_card_query_from_year(self, last_processed_timestamp=None, last_processed_customer_id=None):
        """Get card query starting from specified year with cursor-based pagination"""
        
        # Build WHERE clause for pagination
        pagination_filter = f"WHERE YEAR(CA.TUN_DATE) >= {self.start_year}"
        if last_processed_timestamp and last_processed_customer_id:
            pagination_filter += f"""
            AND (CA.TUN_DATE > DATE('{last_processed_timestamp}') 
                 OR (CA.TUN_DATE = DATE('{last_processed_timestamp}') AND CA.FK_CUST_ID > '{last_processed_customer_id}'))
            """
        
        return f"""
        SELECT
            CURRENT_TIMESTAMP AS reportingDate,
            'MWCOTZTZ' AS bankCode,
            CA.FULL_CARD_NO AS cardNumber,
            RIGHT(TRIM(CA.FULL_CARD_NO), 10) AS binNumber,
            CA.FK_CUST_ID AS customerIdentificationNumber,
            'Debit' AS cardType,
            NULL AS cardTypeSubCategory,
            CA.TUN_DATE AS cardIssueDate,
            'Mwalimu Commercial Bank Plc' AS cardIssuer,
            'Domestic' AS cardIssuerCategory,
            'TANZANIA, UNITED REPUBLIC OF' AS cardIssuerCountry,
            CA.CARD_NAME_LATIN AS cardHolderName,
            CASE
                WHEN CURRENT_DATE > CA.CARD_EXPIRY_DATE then 'Active'
                ELSE 'Inactive'
            END AS cardStatus,
            'VISA' AS cardScheme,
            'UBX Tanzania Limited' AS acquiringPartner,
            CA.CARD_EXPIRY_DATE AS cardExpireDate,
            CA.TUN_DATE AS rawTimestamp
        FROM CMS_CARD CA
        {pagination_filter}
        ORDER BY CA.TUN_DATE ASC, CA.FK_CUST_ID ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
    
    def get_total_count(self):
        """Get total count of card records from start year"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = f"""
                SELECT COUNT(*) 
                FROM CMS_CARD CA
                WHERE YEAR(CA.TUN_DATE) >= {self.start_year}
                """
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def run_complete_pipeline(self):
        """Run the complete card pipeline from 2016"""
        self.logger.info("🚀 Starting Complete Card Pipeline from 2016")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Clear existing data
            self.logger.info("🗑️ Clearing existing card data...")
            self.clear_existing_data()
            
            # Step 2: Get total count
            self.logger.info(f"📊 Getting total card records from {self.start_year}...")
            total_records = self.get_total_count()
            self.logger.info(f"📊 Total card records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("ℹ️ No card records found from specified year")
                return
            
            # Step 3: Process in batches using cursor-based pagination
            total_processed = 0
            batch_number = 1
            last_processed_timestamp = None
            last_processed_customer_id = None
            
            while True:
                self.logger.info(f"\n📊 Batch {batch_number}: Fetching next batch of records...")
                
                # Fetch batch
                with self.get_db2_connection() as conn:
                    cursor = conn.cursor()
                    
                    card_query = self.get_card_query_from_year(last_processed_timestamp, last_processed_customer_id)
                    self.logger.info("📊 Executing card query...")
                    
                    cursor.execute(card_query)
                    rows = cursor.fetchall()
                    
                    self.logger.info(f"💳 Fetched {len(rows)} card records in batch {batch_number}")
                    
                    if not rows:
                        self.logger.info("ℹ️ No more records found - processing complete!")
                        break
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample card records:")
                        for i, row in enumerate(rows[:min(3, len(rows))], 1):
                            # Mask card number for security
                            card_number = str(row[2])
                            masked_card = card_number[:4] + '****' + card_number[-4:] if len(card_number) >= 8 else card_number
                            self.logger.info(f"  {i}. Card: {masked_card} | Customer: {row[4]} | Type: {row[5]} | Status: {row[12]}")
                
                # Process batch
                batch_processed = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    for row in rows:
                        try:
                            # Skip reportingDate for processor - it expects the 15 fields from bankCode through cardExpireDate
                            # Note: row now has 17 fields (added rawTimestamp), so we take fields 1-16
                            adjusted_row = row[1:16]  # Remove reportingDate, keep fields 1-15 (bankCode through cardExpireDate)
                            record = self.card_processor.process_record(adjusted_row, 'card_information')
                            
                            if self.card_processor.validate_record(record):
                                self.card_processor.insert_to_postgres(record, pg_cursor)
                                conn.commit()
                                batch_processed += 1
                                
                                if batch_processed % 100 == 0:
                                    self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                            else:
                                self.logger.warning(f"⚠️ Invalid card record skipped: {record.card_number}")
                                
                        except Exception as e:
                            self.logger.error(f"❌ Error processing card record: {e}")
                            conn.rollback()
                            continue
                    
                    # Update pagination cursors from last row
                    if rows:
                        last_row = rows[-1]
                        last_processed_timestamp = str(last_row[16])  # rawTimestamp is at index 16
                        last_processed_customer_id = str(last_row[4])  # customerIdentificationNumber is at index 4
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} records processed")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
                self.logger.info(f"📅 Last processed: {last_processed_timestamp} | Customer: {last_processed_customer_id}")
                
                # Move to next batch
                batch_number += 1
                
                # Small delay between batches
                import time
                time.sleep(1)
            
            # Step 4: Final verification
            self.logger.info(f"\n🎉 COMPLETE CARD PIPELINE FINISHED!")
            self.logger.info(f"📊 Total records processed: {total_processed}")
            self.logger.info(f"📊 Total batches: {batch_number - 1}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM "cardInformation";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"📊 Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data
                cursor.execute("""
                    SELECT "cardNumber", "cardHolderName", "cardType", "cardStatus", "cardIssueDate"
                    FROM "cardInformation" 
                    ORDER BY "cardIssueDate" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("📋 Sample of processed records:")
                for record in sample_records:
                    card_number, card_holder_name, card_type, card_status, card_issue_date = record
                    # Mask card number for security
                    masked_card = card_number[:4] + '****' + card_number[-4:] if len(card_number) >= 8 else card_number
                    self.logger.info(f"  💳 {masked_card} | {card_holder_name} | {card_type} | {card_status} | {card_issue_date}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    # Configuration
    START_YEAR = 2016
    BATCH_SIZE = 1000  # Process 1000 records per batch
    
    print("💳 Complete Card Pipeline from 2016 - BOT Project")
    print("=" * 60)
    print(f"📅 Starting Year: {START_YEAR}")
    print(f"📊 Batch Size: {BATCH_SIZE}")
    print("=" * 60)
    
    pipeline = CardPipeline2016(start_year=START_YEAR, limit=BATCH_SIZE)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()