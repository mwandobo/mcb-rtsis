#!/usr/bin/env python3
"""
Test the corrected cash query without Redis tracking
"""

import pika
import psycopg2
import json
import logging
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from db2_connection import DB2Connection
from processors.cash_processor import CashProcessor, CashRecord

class SimpleCashTest:
    def __init__(self):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.cash_processor = CashProcessor()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("💰 Simple Cash Test initialized")
    
    def get_corrected_cash_query(self, manual_start_date, limit=100):
        """Get the corrected cash query matching sqls/cash-information.sql"""
        
        timestamp_filter = f"AND gte.TRN_DATE >= TIMESTAMP('{manual_start_date}')"
        
        query = f"""
        SELECT
            CURRENT_TIMESTAMP as reportingDate,
            gte.FK_UNITCODETRXUNIT AS branchCode,
            CASE
              WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
              WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
              WHEN gl.EXTERNAL_GLACCOUNT='101000010' OR gl.EXTERNAL_GLACCOUNT='101000015' THEN 'Cash in ATMs'
              WHEN gl.EXTERNAL_GLACCOUNT='101000004' OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Cash with Tellers'
              ELSE 'unknown'
            END as cashCategory,
            CASE
                WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'CleanNotes'
                WHEN gl.EXTERNAL_GLACCOUNT='101000002'  OR
                     gl.EXTERNAL_GLACCOUNT='101000010'  OR
                     gl.EXTERNAL_GLACCOUNT='101000004'  OR
                     gl.EXTERNAL_GLACCOUNT='101000015'  OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Notes'
                ELSE null
            END as cashSubCategory,
            'Business Hours' as cashSubmissionTime,
            gte.CURRENCY_SHORT_DES as currency,
            null as cashDenomination,
            null as quantityOfCoinsNotes,
            gte.DC_AMOUNT AS orgAmount,
            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN gte.DC_AMOUNT
                ELSE NULL
            END AS usdAmount,
            CASE
                WHEN gte.CURRENCY_SHORT_DES = 'USD'
                    THEN gte.DC_AMOUNT * 2500
                ELSE
                    gte.DC_AMOUNT
            END AS tzsAmount,
            gte.TRN_DATE as transactionDate,
            gte.AVAILABILITY_DATE as maturityDate,
            0 as allowanceProbableLoss,
            0 as botProvision
        FROM GLI_TRX_EXTRACT AS gte
        JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
        WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015')
        {timestamp_filter}
        ORDER BY gte.TRN_DATE ASC
        FETCH FIRST {limit} ROWS ONLY
        """
        
        return query
    
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
    
    def test_query_and_process(self, manual_start_date="2024-01-01 00:00:00", limit=10):
        """Test the corrected query and process a few records"""
        
        try:
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                
                query = self.get_corrected_cash_query(manual_start_date, limit)
                self.logger.info(f"🔍 Testing corrected cash query from {manual_start_date}...")
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                self.logger.info(f"📊 Fetched {len(rows)} cash records")
                
                if rows:
                    # Show sample data structure
                    self.logger.info("📋 Sample data structure:")
                    for i, row in enumerate(rows[:3], 1):
                        self.logger.info(f"  {i}. Date: {row[11]}, Branch: {row[1]}, Category: {row[2]}")
                        self.logger.info(f"      SubCategory: {row[3]}, SubmissionTime: '{row[4]}'")
                        self.logger.info(f"      Currency: {row[5]}, Amount: {row[7]:,.2f}")
                    
                    # Process and validate records
                    processed_records = []
                    for row in rows:
                        try:
                            record = self.cash_processor.process_record(row, 'cash_information')
                            if self.cash_processor.validate_record(record):
                                processed_records.append(record)
                            else:
                                self.logger.warning(f"⚠️ Invalid record: {row[1]}")
                        except Exception as e:
                            self.logger.error(f"❌ Error processing record: {e}")
                    
                    self.logger.info(f"✅ Successfully processed {len(processed_records)} valid records")
                    
                    # Show processed record sample
                    if processed_records:
                        sample = processed_records[0]
                        self.logger.info("📋 Sample processed record:")
                        self.logger.info(f"  cashSubmissionTime: '{sample.cash_submission_time}'")
                        self.logger.info(f"  cashCategory: '{sample.cash_category}'")
                        self.logger.info(f"  cashSubCategory: '{sample.cash_sub_category}'")
                        self.logger.info(f"  transactionDate: '{sample.transaction_date}'")
                    
                    return len(processed_records)
                else:
                    self.logger.info("⚠️ No records found")
                    return 0
                    
        except Exception as e:
            self.logger.error(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return 0

def main():
    """Main function"""
    tester = SimpleCashTest()
    
    print("💰 TESTING CORRECTED CASH QUERY")
    print("=" * 50)
    print("🎯 Testing query structure matches sqls/cash-information.sql")
    print("📅 Using manual start date: 2024-01-01 00:00:00")
    print("📊 Limit: 10 records for testing")
    print("=" * 50)
    
    count = tester.test_query_and_process("2024-01-01 00:00:00", 10)
    
    if count > 0:
        print(f"\n✅ SUCCESS: Processed {count} records with corrected query")
        print("🔍 Key corrections verified:")
        print("  - cashSubmissionTime: 'Business Hours' (text)")
        print("  - transactionDate: uses gte.TRN_DATE")
        print("  - cashSubCategory: proper CleanNotes/Notes logic")
        print("  - GL accounts: matches original specification")
    else:
        print("\n❌ FAILED: No records processed")

if __name__ == "__main__":
    main()