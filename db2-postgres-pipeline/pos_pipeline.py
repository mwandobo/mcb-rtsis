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
        
        # Read the POS query from pos-v1.sql file
        import os
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sqls', 'pos-v1.sql')
        with open(sql_file_path, 'r') as f:
            self.base_pos_query = f.read()
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.pos_processor = POSProcessor()
        
        self.logger.info(f"🏪 POS Pipeline initialized")
        self.logger.info(f"📊 Batch limit: {self.limit}")
        self.logger.info(f"📄 Using pos-v1.sql query")
        
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
        """Get POS query with cursor-based pagination using pos-v1.sql"""
        
        # Build WHERE clause for pagination
        pagination_filter = ""
        if last_processed_user_code:
            pagination_filter = f"AND at.FK_USRCODE > '{last_processed_user_code}'"
        
        # Add WHERE clause and pagination to the base query
        query_with_pagination = self.base_pos_query
        
        # Add WHERE clause if not present
        if "WHERE" not in query_with_pagination.upper():
            query_with_pagination += f"\nWHERE 1=1 {pagination_filter}"
        else:
            query_with_pagination += f" {pagination_filter}"
        
        # Add ORDER BY and FETCH FIRST
        query_with_pagination += f"\nORDER BY at.FK_USRCODE ASC\nFETCH FIRST {self.limit} ROWS ONLY"
        
        return query_with_pagination
    
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
            
            # Step 3: Fetch all records at once (pos-v1.sql creates duplicates due to joins)
            self.logger.info("📊 Fetching all POS records (handling duplicates from joins)...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the base query without pagination to get all records
                cursor.execute(self.base_pos_query)
                all_rows = cursor.fetchall()
                
                self.logger.info(f"🏪 Fetched {len(all_rows)} total records from pos-v1.sql (includes duplicates from joins)")
            
            # Step 4: Process records and handle duplicates
            processed_pos_numbers = set()
            unique_records = []
            
            for row in all_rows:
                pos_number = str(row[2])  # posNumber is at index 2
                if pos_number not in processed_pos_numbers:
                    unique_records.append(row)
                    processed_pos_numbers.add(pos_number)
            
            self.logger.info(f"📊 Unique POS records after deduplication: {len(unique_records)}")
            self.logger.info(f"📊 Duplicate records filtered out: {len(all_rows) - len(unique_records)}")
            
            # Show sample data
            self.logger.info("📋 Sample unique POS records:")
            for i, row in enumerate(unique_records[:min(3, len(unique_records))], 1):
                self.logger.info(f"  {i}. POS: {row[2]} | QR Code: {row[3]} | Region: {row[8]} | District: {row[9]}")
            
            # Step 5: Process unique records in batches
            total_processed = 0
            batch_size = 500  # Process in smaller batches for better performance
            
            for batch_start in range(0, len(unique_records), batch_size):
                batch_end = min(batch_start + batch_size, len(unique_records))
                batch_records = unique_records[batch_start:batch_end]
                batch_number = (batch_start // batch_size) + 1
                
                self.logger.info(f"\n📊 Processing batch {batch_number}: records {batch_start + 1} to {batch_end}")
                
                batch_processed = 0
                batch_skipped = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    try:
                        for row in batch_records:
                            try:
                                # Process the record using the processor
                                record = self.pos_processor.process_record(row, self.table_config.name)
                                
                                if self.pos_processor.validate_record(record):
                                    self.pos_processor.insert_to_postgres(record, pg_cursor)
                                    batch_processed += 1
                                    
                                    if batch_processed % 100 == 0:
                                        self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                                else:
                                    self.logger.warning(f"⚠️ Invalid POS record skipped: {record.pos_number}")
                                    batch_skipped += 1
                                    
                            except Exception as e:
                                self.logger.error(f"❌ Error processing POS record: {e}")
                                batch_skipped += 1
                                continue
                        
                        # Commit the entire batch at once
                        conn.commit()
                        self.logger.info(f"✅ Batch {batch_number} committed successfully")
                        
                    except Exception as e:
                        self.logger.error(f"❌ Batch {batch_number} failed, rolling back: {e}")
                        conn.rollback()
                        batch_processed = 0
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} new records, {batch_skipped} skipped")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
            
            # Step 6: Final verification
            self.logger.info(f"\n🎉 COMPLETE POS PIPELINE FINISHED!")
            self.logger.info(f"📊 Total unique records processed: {total_processed}")
            self.logger.info(f"📊 Expected unique POS terminals: {total_records}")
            
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