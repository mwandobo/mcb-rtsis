#!/usr/bin/env python3
"""
ATM Pipeline Starting from 2016 - BOT Project
Simple pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.atm_processor import AtmProcessor

class AtmPipeline2016:
    def __init__(self, start_year=2016, limit=10000):
        """
        ATM Pipeline starting from specified year
        
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
        self.atm_processor = AtmProcessor()
        
        self.logger.info(f"🏧 ATM Pipeline initialized - Starting from {start_year}")
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
        """Clear existing ATM data from PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM "atmInformation";')
                conn.commit()
                self.logger.info("🗑️ Cleared existing ATM data from PostgreSQL")
        except Exception as e:
            self.logger.error(f"❌ Failed to clear existing data: {e}")
            raise
    
    def get_atm_query_from_year(self, last_processed_timestamp=None, last_processed_staff_no=None):
        """Get ATM query starting from specified year with cursor-based pagination"""
        
        # Build WHERE clause for pagination
        pagination_filter = ""
        if last_processed_timestamp and last_processed_staff_no:
            pagination_filter = f"""
            AND (be.TMSTAMP > TIMESTAMP('{last_processed_timestamp}') 
                 OR (be.TMSTAMP = TIMESTAMP('{last_processed_timestamp}') AND be.STAFF_NO > '{last_processed_staff_no}'))
            """
        
        return f"""
        SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                    AS reportingDate,
               be.FIRST_NAME                                                        AS atmName,
               b.branchCode,
               be.STAFF_NO                                                          AS atmCode,
               CASE WHEN b.branchCode = 200 THEN '200' ELSE '300' END               AS tillNumber,
               'M-Pesa'                                                             AS mobileMoneyServices,
               'FSR-' || CAST(be.STAFF_NO AS VARCHAR(10))                           AS qrFsrCode,
               NULL                                                                 AS postalCode,
               'DAR ES SALAAM'                                                      AS region,
               CASE WHEN b.branchCode = 200 THEN 'ILALA' ELSE 'UBUNGO' END          AS district,
               CASE WHEN b.branchCode = 200 THEN 'KISUTU' ELSE 'UBUNGO WARD' END    AS ward,
               CASE WHEN b.branchCode = 200 THEN 'SAMORA STREET' ELSE 'MLIMANI' END AS street,
               NULL                                                                 AS houseNumber,
               CASE
                   WHEN u.LATITUDE_LOCATION IS NOT NULL AND u.LONGITUDE_LOCATION IS NOT NULL
                       THEN TRIM(u.LATITUDE_LOCATION) || ',' || TRIM(u.LONGITUDE_LOCATION)
                   WHEN u.GEO_AREA IS NOT NULL
                       THEN u.GEO_AREA
                   ELSE '0.0000,0.0000'
                   END                                                              AS gpsCoordinates,
               CASE WHEN b.branchCode = 200 THEN '101000010' ELSE '101000015' END   AS linkedAccount,
               VARCHAR_FORMAT(be.TMSTAMP, 'DDMMYYYYHHMM')                           AS openingDate,
               'active'                                                             AS atmStatus,
               null                                                                 AS closureDate,
               'onsite'                                                             AS atmCategory,
               'Card and Mobile Based'                                              AS atmChannel,
               be.TMSTAMP                                                           AS rawTimestamp
        FROM BANKEMPLOYEE be
                 JOIN (SELECT STAFF_NO,
                              CASE
                                  WHEN STAFF_NO = 'MWL01001' THEN 200
                                  ELSE 201
                                  END AS branchCode
                       FROM BANKEMPLOYEE) b
                      ON b.STAFF_NO = be.STAFF_NO
                 JOIN UNIT u
                      ON u.CODE = b.branchCode
        WHERE be.STAFF_NO IS NOT NULL
          AND be.STAFF_NO = TRIM(be.STAFF_NO)
          AND be.EMPL_STATUS = 1
          AND be.STAFF_NO LIKE 'MWL01%'
          AND u.CODE IN (200, 201)
          AND YEAR(be.TMSTAMP) >= {self.start_year}
          {pagination_filter}
        ORDER BY be.TMSTAMP ASC, be.STAFF_NO ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
    
    def get_total_count(self):
        """Get total count of ATM records from start year"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = f"""
                SELECT COUNT(*) 
                FROM BANKEMPLOYEE be
                         JOIN (SELECT STAFF_NO,
                                      CASE
                                          WHEN STAFF_NO = 'MWL01001' THEN 200
                                          ELSE 201
                                          END AS branchCode
                               FROM BANKEMPLOYEE) b
                              ON b.STAFF_NO = be.STAFF_NO
                         JOIN UNIT u
                              ON u.CODE = b.branchCode
                WHERE be.STAFF_NO IS NOT NULL
                  AND be.STAFF_NO = TRIM(be.STAFF_NO)
                  AND be.EMPL_STATUS = 1
                  AND be.STAFF_NO LIKE 'MWL01%'
                  AND u.CODE IN (200, 201)
                  AND YEAR(be.TMSTAMP) >= {self.start_year}
                """
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def run_complete_pipeline(self):
        """Run the complete ATM pipeline from 2016"""
        self.logger.info("🚀 Starting Complete ATM Pipeline from 2016")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Clear existing data
            self.logger.info("🗑️ Clearing existing ATM data...")
            self.clear_existing_data()
            
            # Step 2: Get total count
            self.logger.info(f"📊 Getting total ATM records from {self.start_year}...")
            total_records = self.get_total_count()
            self.logger.info(f"📊 Total ATM records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("ℹ️ No ATM records found from specified year")
                return
            
            # Step 3: Process in batches using cursor-based pagination
            total_processed = 0
            batch_number = 1
            last_processed_timestamp = None
            last_processed_staff_no = None
            
            while True:
                self.logger.info(f"\n📊 Batch {batch_number}: Fetching next batch of records...")
                
                # Fetch batch
                with self.get_db2_connection() as conn:
                    cursor = conn.cursor()
                    
                    atm_query = self.get_atm_query_from_year(last_processed_timestamp, last_processed_staff_no)
                    self.logger.info("📊 Executing ATM query...")
                    
                    cursor.execute(atm_query)
                    rows = cursor.fetchall()
                    
                    self.logger.info(f"🏧 Fetched {len(rows)} ATM records in batch {batch_number}")
                    
                    if not rows:
                        self.logger.info("ℹ️ No more records found - processing complete!")
                        break
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample ATM records:")
                        for i, row in enumerate(rows[:min(3, len(rows))], 1):
                            self.logger.info(f"  {i}. ATM: {row[3]} ({row[1]}) | Branch: {row[2]} | Status: {row[16]} | Account: {row[14]}")
                
                # Process batch
                batch_processed = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    for row in rows:
                        try:
                            # Skip reportingDate for processor - it expects the 19 fields from atmName through atmChannel
                            # Note: row now has 21 fields (added rawTimestamp), so we take fields 1-19
                            adjusted_row = row[1:20]  # Remove reportingDate, keep fields 1-19 (atmName through atmChannel)
                            record = self.atm_processor.process_record(adjusted_row, 'atm_information')
                            
                            if self.atm_processor.validate_record(record):
                                self.atm_processor.insert_to_postgres(record, pg_cursor)
                                conn.commit()
                                batch_processed += 1
                                
                                if batch_processed % 100 == 0:
                                    self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                            else:
                                self.logger.warning(f"⚠️ Invalid ATM record skipped: {record.atm_code}")
                                
                        except Exception as e:
                            self.logger.error(f"❌ Error processing ATM record: {e}")
                            conn.rollback()
                            continue
                    
                    # Update pagination cursors from last row
                    if rows:
                        last_row = rows[-1]
                        last_processed_timestamp = str(last_row[20])  # rawTimestamp is at index 20
                        last_processed_staff_no = str(last_row[3])    # atmCode is at index 3
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} records processed")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
                self.logger.info(f"📅 Last processed: {last_processed_timestamp} | Staff: {last_processed_staff_no}")
                
                # Move to next batch
                batch_number += 1
                
                # Small delay between batches
                import time
                time.sleep(1)
            
            # Step 4: Final verification
            self.logger.info(f"\n🎉 COMPLETE ATM PIPELINE FINISHED!")
            self.logger.info(f"📊 Total records processed: {total_processed}")
            self.logger.info(f"📊 Total batches: {batch_number - 1}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM "atmInformation";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"📊 Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data
                cursor.execute("""
                    SELECT "atmCode", "atmName", "branchCode", "atmStatus", "openingDate"
                    FROM "atmInformation" 
                    ORDER BY "openingDate" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("📋 Sample of processed records:")
                for record in sample_records:
                    atm_code, atm_name, branch_code, atm_status, opening_date = record
                    self.logger.info(f"  🏧 {atm_code} | {atm_name} | Branch: {branch_code} | {opening_date}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    # Configuration
    START_YEAR = 2016
    BATCH_SIZE = 1000  # Process 1000 records per batch
    
    print("🏧 Complete ATM Pipeline from 2016 - BOT Project")
    print("=" * 60)
    print(f"📅 Starting Year: {START_YEAR}")
    print(f"📊 Batch Size: {BATCH_SIZE}")
    print("=" * 60)
    
    pipeline = AtmPipeline2016(start_year=START_YEAR, limit=BATCH_SIZE)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()