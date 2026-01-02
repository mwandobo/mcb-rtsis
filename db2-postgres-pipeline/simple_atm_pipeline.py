#!/usr/bin/env python3
"""
Simple ATM Pipeline Test - BOT Project
Basic ATM data pipeline without RabbitMQ for testing
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.atm_processor import AtmProcessor

class SimpleAtmPipeline:
    def __init__(self, limit=100):
        """
        Simple ATM Pipeline for testing
        
        Args:
            limit (int): Number of records to fetch
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.limit = limit
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.atm_processor = AtmProcessor()
        
        self.logger.info("🏧 Simple ATM Pipeline initialized")
        self.logger.info(f"📊 Record limit: {limit}")
        
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
    
    def get_atm_query(self):
        """Get simple ATM query - Updated to match existing ATM SQL"""
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
               'Card and Mobile Based'                                              AS atmChannel
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
        ORDER BY be.STAFF_NO ASC
        FETCH FIRST {self.limit} ROWS ONLY
        """
    
    def run_simple_pipeline(self):
        """Run the simple ATM pipeline"""
        self.logger.info("🚀 Starting Simple ATM Pipeline")
        self.logger.info("=" * 50)
        
        try:
            # Step 1: Fetch data from DB2
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                atm_query = self.get_atm_query()
                self.logger.info("📊 Executing ATM query...")
                
                cursor.execute(atm_query)
                rows = cursor.fetchall()
                
                self.logger.info(f"🏧 Fetched {len(rows)} ATM records")
                
                if not rows:
                    self.logger.info("ℹ️ No ATM records found")
                    return
                
                # Show sample data
                self.logger.info("📋 Sample ATM records:")
                for i, row in enumerate(rows[:3], 1):
                    self.logger.info(f"  {i}. ATM: {row[3]} ({row[1]}) | Branch: {row[2]} | Status: {row[16]} | Account: {row[14]}")
            
            # Step 2: Process and insert to PostgreSQL
            processed_count = 0
            
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                
                for row in rows:
                    try:
                        # Skip reportingDate for processor - it expects the 18 fields from atmName through atmChannel
                        adjusted_row = row[1:]  # Remove reportingDate, keep the 18 fields
                        record = self.atm_processor.process_record(adjusted_row, 'atm_information')
                        
                        if self.atm_processor.validate_record(record):
                            self.atm_processor.insert_to_postgres(record, cursor)
                            conn.commit()
                            processed_count += 1
                            
                            if processed_count % 10 == 0:
                                self.logger.info(f"✅ Processed {processed_count} ATM records...")
                        else:
                            self.logger.warning(f"⚠️ Invalid ATM record skipped: {record.atm_code}")
                            
                    except Exception as e:
                        self.logger.error(f"❌ Error processing ATM record: {e}")
                        conn.rollback()
                        continue
            
            # Step 3: Show final status
            self.logger.info(f"\n🎉 ATM PIPELINE COMPLETED!")
            self.logger.info(f"📊 Total records processed: {processed_count}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    # Configuration
    RECORD_LIMIT = 100  # Process up to 100 records for testing
    
    print("🏧 Simple ATM Pipeline - BOT Project")
    print("=" * 40)
    print(f"📊 Record Limit: {RECORD_LIMIT}")
    print("=" * 40)
    
    pipeline = SimpleAtmPipeline(limit=RECORD_LIMIT)
    pipeline.run_simple_pipeline()

if __name__ == "__main__":
    main()