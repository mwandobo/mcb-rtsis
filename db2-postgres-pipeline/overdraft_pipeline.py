#!/usr/bin/env python3
"""
Overdraft Pipeline
Processes overdraft data from DB2 to PostgreSQL
"""

import logging
import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.overdraft_processor import OverdraftProcessor
import psycopg2
from psycopg2.extras import RealDictCursor

class OverdraftPipeline:
    def __init__(self):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.processor = OverdraftProcessor()
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/overdraft_pipeline.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)

    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        try:
            conn = psycopg2.connect(
                host=self.config.database.pg_host,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password,
                port=self.config.database.pg_port
            )
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def run_pipeline(self, limit=None):
        """Run the overdraft pipeline"""
        self.logger.info("Starting Overdraft Pipeline")
        
        try:
            # Get overdraft configuration
            overdraft_config = self.config.tables['overdraft']
            
            # Connect to DB2
            with self.db2_conn.get_connection() as db2_conn:
                cursor = db2_conn.cursor()
                
                # Prepare query with optional limit
                query = overdraft_config.query
                if limit:
                    query += f" FETCH FIRST {limit} ROWS ONLY"
                
                self.logger.info(f"Executing overdraft query...")
                cursor.execute(query)
                
                # Connect to PostgreSQL
                with self.get_postgres_connection() as pg_conn:
                    pg_cursor = pg_conn.cursor()
                    
                    # Process records in batches
                    batch_size = overdraft_config.batch_size
                    processed_count = 0
                    error_count = 0
                    
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                            
                        self.logger.info(f"Processing batch of {len(rows)} overdraft records")
                        
                        for row in rows:
                            try:
                                # Process the record
                                record = self.processor.process_record(row, 'overdraft')
                                
                                # Validate the record
                                if self.processor.validate_record(record):
                                    # Insert to PostgreSQL
                                    self.processor.insert_to_postgres(record, pg_cursor)
                                    processed_count += 1
                                else:
                                    self.logger.warning(f"Invalid record skipped: {record.account_number}")
                                    error_count += 1
                                    
                            except Exception as e:
                                self.logger.error(f"Error processing record: {e}")
                                error_count += 1
                                continue
                        
                        # Commit batch
                        pg_conn.commit()
                        self.logger.info(f"Committed batch. Total processed: {processed_count}")
                    
                    self.logger.info(f"Overdraft pipeline completed!")
                    self.logger.info(f"Successfully processed: {processed_count} records")
                    if error_count > 0:
                        self.logger.warning(f"Errors encountered: {error_count} records")
                        
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise

def main():
    """Main function"""
    pipeline = OverdraftPipeline()
    
    # Check command line arguments for limit
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"Running with limit: {limit} records")
        except ValueError:
            print("Invalid limit argument. Using no limit.")
    
    try:
        pipeline.run_pipeline(limit=limit)
        print("Overdraft pipeline completed successfully!")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()