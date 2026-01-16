#!/usr/bin/env python3
"""
Cheque Clearing Pipeline - BOT Project
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.cheque_clearing_processor import ChequeClearingProcessor

class ChequeClearingPipeline:
    def __init__(self, limit=None):
        """Cheque Clearing Pipeline"""
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get cheque clearing table config
        self.table_config = self.config.tables.get('chequeClearing')
        if not self.table_config:
            raise ValueError("Cheque clearing table config not found")
        
        self.limit = limit or self.table_config.batch_size
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.processor = ChequeClearingProcessor()
        
        self.logger.info(f"Cheque Clearing Pipeline initialized")
        self.logger.info(f"Batch limit: {self.limit}")
    
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
    
    def run(self):
        """Run the cheque clearing pipeline"""
        self.logger.info("=" * 60)
        self.logger.info("CHEQUE CLEARING PIPELINE")
        self.logger.info("=" * 60)
        
        try:
            # Get existing count
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}"')
                existing_count = cursor.fetchone()[0]
                self.logger.info(f"Existing records in PostgreSQL: {existing_count}")
            
            # Fetch data from DB2
            self.logger.info("Fetching cheque clearing records from DB2...")
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.table_config.query)
                rows = cursor.fetchall()
                
                self.logger.info(f"Fetched {len(rows)} cheque clearing records")
                
                if not rows:
                    self.logger.info("No records found")
                    return
                
                # Show sample
                self.logger.info("Sample records:")
                for i, row in enumerate(rows[:3], 1):
                    self.logger.info(f"  {i}. Cheque: {row[1]} | Issuer: {row[2]} | Amount: {row[16]} {row[13]}")
            
            # Process records
            processed_count = 0
            skipped_count = 0
            
            with self.get_postgres_connection() as conn:
                pg_cursor = conn.cursor()
                
                for i, row in enumerate(rows, 1):
                    try:
                        record = self.processor.process_record(row, self.table_config.name)
                        
                        if self.processor.validate_record(record):
                            self.processor.insert_to_postgres(record, pg_cursor)
                            processed_count += 1
                            
                            if processed_count % 100 == 0:
                                self.logger.info(f"Processed {processed_count}/{len(rows)} records...")
                        else:
                            skipped_count += 1
                            
                    except Exception as e:
                        self.logger.error(f"Error processing record {i}: {e}")
                        skipped_count += 1
                        continue
                
                # Commit all changes
                conn.commit()
                self.logger.info(f"All records committed successfully")
            
            # Final count
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}"')
                final_count = cursor.fetchone()[0]
            
            self.logger.info("=" * 60)
            self.logger.info("CHEQUE CLEARING PIPELINE FINISHED!")
            self.logger.info(f"Total records processed: {processed_count}")
            self.logger.info(f"Records skipped: {skipped_count}")
            self.logger.info(f"Final count in PostgreSQL: {final_count}")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise

def main():
    pipeline = ChequeClearingPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()
