#!/usr/bin/env python3
"""
POS Information Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.pos_processor import POSProcessor

class POSPipeline:
    def __init__(self, limit=None):
        """
        POS Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get POS table config
        self.table_config = self.config.tables.get('pos_information')
        if not self.table_config:
            raise ValueError("POS information table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.pos_processor = POSProcessor()
        
        self.logger.info(f"🏪 POS Pipeline initialized")
        self.logger.info(f"📊 Batch limit: {self.limit}")
        
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
        """Clear existing POS data from PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'DELETE FROM "{self.table_config.target_table}";')
                conn.commit()
                self.logger.info("🗑️ Cleared existing POS data from PostgreSQL")
        except Exception as e:
            self.logger.error(f"❌ Failed to clear existing data: {e}")
            raise
    
    def get_pos_query(self, last_processed_user_code=None):
        """Get POS query with cursor-based pagination"""
        
        # Build WHERE clause for pagination
        pagination_filter = ""
        if last_processed_user_code:
            pagination_filter = f"AND at.FK_USRCODE > '{last_processed_user_code}'"
        
        # Build the complete query with pagination
        return f"""
        SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
               201                                                  AS posBranchCode,
               at.FK_USRCODE                                        AS posNumber,
               'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
               'Selcom Paytech Ltd'                                 AS posHolderName,
               NULL                                                 AS posHolderNin,
               '103847451'                                          AS posHolderTin,
               NULL                                                 AS postalCode,
               COALESCE(
                       dl.REGION,
                       (SELECT r.REGION
                        FROM PROFITS.BANK_LOCATION_LOOKUP r
                        WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(r.REGION) || '%'
                            FETCH FIRST 1 ROW ONLY),
                       'DAR ES SALAAM' -- final hardcoded fallback
               )                                                    AS region,
               COALESCE(
                       dl.DISTRICT,
                       (SELECT r.DISTRICT
                        FROM PROFITS.BANK_LOCATION_LOOKUP r
                        WHERE UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(r.REGION) || '%'
                            FETCH FIRST 1 ROW ONLY),
                       'ILALA' -- final hardcoded fallback
               )                                                    AS district,
               NULL                                                 AS ward,
               NULL                                                 AS street,
               NULL                                                 AS houseNumber,
               NULL                                                 AS gpsCoordinates,
               '230000070'                                          AS linkedAccount,
               VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
               NULL                                                 AS returnDate
        FROM PROFITS.AGENT_TERMINAL at
                 LEFT JOIN PROFITS.BANK_LOCATION_LOOKUP dl
                           ON UPPER(TRIM(at.LOCATION)) LIKE '%' || UPPER(dl.DISTRICT) || '%'
        WHERE 1=1
        {pagination_filter}
        ORDER BY at.FK_USRCODE ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
    
    def get_total_count(self):
        """Get total count of POS records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = """
                SELECT COUNT(*) 
                FROM PROFITS.AGENT_TERMINAL at
                """
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def run_complete_pipeline(self):
        """Run the complete POS pipeline"""
        self.logger.info("🚀 Starting Complete POS Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Clear existing data
            self.logger.info("🗑️ Clearing existing POS data...")
            self.clear_existing_data()
            
            # Step 2: Get total count
            self.logger.info("📊 Getting total POS records...")
            total_records = self.get_total_count()
            self.logger.info(f"📊 Total POS records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("ℹ️ No POS records found")
                return
            
            # Step 3: Process in batches using cursor-based pagination
            total_processed = 0
            batch_number = 1
            last_processed_user_code = None
            
            while True:
                self.logger.info(f"\n📊 Batch {batch_number}: Fetching next batch of records...")
                
                # Fetch batch
                with self.get_db2_connection() as conn:
                    cursor = conn.cursor()
                    
                    pos_query = self.get_pos_query(last_processed_user_code)
                    self.logger.info("📊 Executing POS query...")
                    
                    cursor.execute(pos_query)
                    rows = cursor.fetchall()
                    
                    self.logger.info(f"🏪 Fetched {len(rows)} POS records in batch {batch_number}")
                    
                    if not rows:
                        self.logger.info("ℹ️ No more records found - processing complete!")
                        break
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample POS records:")
                        for i, row in enumerate(rows[:min(3, len(rows))], 1):
                            self.logger.info(f"  {i}. POS: {row[2]} | QR Code: {row[3]} | Region: {row[8]} | District: {row[9]}")
                
                # Process batch
                batch_processed = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    for row in rows:
                        try:
                            # Process the record using the processor
                            record = self.pos_processor.process_record(row, self.table_config.name)
                            
                            if self.pos_processor.validate_record(record):
                                self.pos_processor.insert_to_postgres(record, pg_cursor)
                                conn.commit()
                                batch_processed += 1
                                
                                if batch_processed % 100 == 0:
                                    self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                            else:
                                self.logger.warning(f"⚠️ Invalid POS record skipped: {record.pos_number}")
                                
                        except Exception as e:
                            self.logger.error(f"❌ Error processing POS record: {e}")
                            conn.rollback()
                            continue
                    
                    # Update pagination cursor from last row
                    if rows:
                        last_row = rows[-1]
                        last_processed_user_code = str(last_row[2])  # posNumber is at index 2
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} records processed")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
                self.logger.info(f"📅 Last processed user code: {last_processed_user_code}")
                
                # Move to next batch
                batch_number += 1
                
                # Small delay between batches
                import time
                time.sleep(1)
            
            # Step 4: Final verification
            self.logger.info(f"\n🎉 COMPLETE POS PIPELINE FINISHED!")
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
                    SELECT "posNumber", "qrFsrCode", "posHolderName", "region", "district", "issueDate"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "posNumber" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("📋 Sample of processed records:")
                for record in sample_records:
                    pos_number, qr_fsr_code, pos_holder_name, region, district, issue_date = record
                    self.logger.info(f"  🏪 POS: {pos_number} | QR: {qr_fsr_code} | {pos_holder_name} | {region}/{district} | {issue_date}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("🏪 Complete POS Pipeline - BOT Project")
    print("=" * 60)
    
    pipeline = POSPipeline()
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()