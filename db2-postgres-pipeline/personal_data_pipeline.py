#!/usr/bin/env python3
"""
Personal Data Information Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.personal_data_processor import PersonalDataProcessor

class PersonalDataPipeline:
    def __init__(self, limit=None, resume=False):
        """
        Personal Data Information Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get personal data table config
        self.table_config = self.config.tables.get('personal_data_information')
        if not self.table_config:
            raise ValueError("Personal data information table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.personal_data_processor = PersonalDataProcessor()
        
        self.logger.info(f"👤 Personal Data Information Pipeline initialized")
        self.logger.info(f"📊 Batch limit: {self.limit}")
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
                    SELECT "customerIdentificationNumber"
                    FROM "{self.table_config.target_table}"
                    ORDER BY "customerIdentificationNumber" DESC
                    LIMIT 1;
                """)
                
                result = cursor.fetchone()
                if result:
                    return str(result[0])
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get last processed record: {e}")
            return None
    
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
    
    def get_personal_data_query(self, last_processed_id=None):
        """Get personal data query with pagination"""
        base_query = self.table_config.query
        
        # Add pagination if we have a last processed record
        if last_processed_id:
            # Insert WHERE clause before ORDER BY
            order_by_pos = base_query.upper().rfind('ORDER BY')
            if order_by_pos > 0:
                where_clause = f" AND c.cust_id > {last_processed_id} "
                base_query = base_query[:order_by_pos] + where_clause + base_query[order_by_pos:]
        
        # Update FETCH FIRST clause with current limit
        fetch_pos = base_query.upper().rfind('FETCH FIRST')
        if fetch_pos > 0:
            base_query = base_query[:fetch_pos] + f"FETCH FIRST {self.limit} ROWS ONLY"
        
        return base_query
    
    def get_total_count(self):
        """Get total count of personal data records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = """
                SELECT COUNT(*) 
                FROM customer c
                """
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def run_complete_pipeline(self):
        """Run the complete personal data information pipeline"""
        self.logger.info("🚀 Starting Complete Personal Data Information Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data and resume point
            existing_count = self.get_existing_count()
            self.logger.info(f"📊 Existing records in PostgreSQL: {existing_count}")
            
            last_processed_id = None
            
            if self.resume and existing_count > 0:
                last_processed_id = self.get_last_processed_record()
                self.logger.info(f"🔄 Resuming from customer ID: {last_processed_id}")
            elif not self.resume and existing_count > 0:
                self.logger.warning("⚠️ Existing data found but resume=False. Use resume=True to continue from last record.")
            
            # Step 2: Get total count
            self.logger.info(f"📊 Getting total personal data records...")
            total_records = self.get_total_count()
            self.logger.info(f"📊 Total personal data records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("ℹ️ No personal data records found")
                return
            
            # Step 3: Process in batches using ID-based pagination
            total_processed = existing_count  # Start count from existing records
            batch_number = 1
            
            while True:
                self.logger.info(f"\n📊 Batch {batch_number}: Fetching next batch of records...")
                
                # Fetch batch
                with self.get_db2_connection() as conn:
                    cursor = conn.cursor()
                    
                    personal_data_query = self.get_personal_data_query(last_processed_id)
                    self.logger.info("📊 Executing personal data query...")
                    
                    cursor.execute(personal_data_query)
                    rows = cursor.fetchall()
                    
                    self.logger.info(f"👤 Fetched {len(rows)} personal data records in batch {batch_number}")
                    
                    if not rows:
                        self.logger.info("ℹ️ No more records found - processing complete!")
                        break
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample personal data records:")
                        for i, row in enumerate(rows[:min(3, len(rows))], 1):
                            # Mask sensitive data for security
                            customer_id = str(row[1])
                            full_name = str(row[5]) if row[5] else "N/A"
                            mobile = str(row[57]) if row[57] else "N/A"
                            if len(mobile) > 6:
                                mobile = mobile[:3] + '****' + mobile[-3:]
                            self.logger.info(f"  {i}. Customer: {customer_id} | Name: {full_name} | Mobile: {mobile}")
                
                # Process batch
                batch_processed = 0
                batch_skipped = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    for row in rows:
                        try:
                            # Process the record using the processor
                            record = self.personal_data_processor.process_record(row, self.table_config.name)
                            
                            if self.personal_data_processor.validate_record(record):
                                # Use upsert which handles duplicates automatically
                                self.personal_data_processor.insert_to_postgres(record, pg_cursor)
                                batch_processed += 1
                                
                                if batch_processed % 100 == 0:
                                    self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                            else:
                                self.logger.warning(f"⚠️ Invalid personal data record skipped: {record.customer_identification_number}")
                                batch_skipped += 1
                                
                        except Exception as e:
                            self.logger.error(f"❌ Error processing personal data record: {e}")
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
                    
                    # Update pagination cursor from last row
                    if rows:
                        last_row = rows[-1]
                        last_processed_id = str(last_row[1])  # customerIdentificationNumber is at index 1
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} new records, {batch_skipped} skipped")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
                self.logger.info(f"📅 Last processed customer ID: {last_processed_id}")
                
                # Move to next batch
                batch_number += 1
                
                # Small delay between batches
                import time
                time.sleep(1)
            
            # Step 4: Final verification
            self.logger.info(f"\n🎉 COMPLETE PERSONAL DATA INFORMATION PIPELINE FINISHED!")
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
                    SELECT "customerIdentificationNumber", "fullNames", "gender", "nationality", "profession"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "customerIdentificationNumber" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("📋 Sample of processed records:")
                for record in sample_records:
                    customer_id, full_name, gender, nationality, profession = record
                    self.logger.info(f"  👤 {customer_id} | {full_name} | {gender} | {nationality} | {profession}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("👤 Complete Personal Data Information Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for personal data processing
    pipeline = PersonalDataPipeline(resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()