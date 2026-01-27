#!/usr/bin/env python3
"""
Simple Personal Data Corporate Pipeline
Direct processing from DB2 to PostgreSQL without streaming
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.personal_data_corporate_processor import PersonalDataCorporateProcessor
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_corporate_pipeline(limit=100):
    """Run the corporate personal data pipeline"""
    
    print("Corporate Personal Data Pipeline")
    print("=" * 50)
    
    try:
        # Load SQL query
        sql_file = os.path.join('..', 'sqls', 'personal_data_corporates-v1.sql')
        if not os.path.exists(sql_file):
            sql_file = os.path.join('sqls', 'personal_data_corporates-v1.sql')
        
        with open(sql_file, 'r') as f:
            query = f.read().strip()
            if query.endswith(';'):
                query = query[:-1]
        
        # Add LIMIT for processing
        test_query = f"""
        SELECT * FROM (
            {query}
        ) AS corporate_data
        ORDER BY customerIdentificationNumber
        FETCH FIRST {limit} ROWS ONLY
        """
        
        print(f"Processing {limit} corporate records...")
        logger.info(f"Query: {test_query[:200]}...")
        
        # Initialize processor
        processor = PersonalDataCorporateProcessor()
        
        # Connect to databases
        config = Config()
        
        # PostgreSQL connection
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_conn.autocommit = False
        
        # DB2 connection
        db2_conn_manager = DB2Connection()
        
        processed_count = 0
        error_count = 0
        
        with db2_conn_manager.get_connection() as db2_conn:
            db2_cursor = db2_conn.cursor()
            
            # Execute query
            db2_cursor.execute(test_query)
            
            batch_size = 10
            while True:
                rows = db2_cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Process batch
                for row in rows:
                    try:
                        # Process the record
                        record = processor.process_record(row, 'personalDataCorporate')
                        
                        # Insert to PostgreSQL
                        pg_cursor = pg_conn.cursor()
                        processor.insert_to_postgres(record, pg_cursor)
                        pg_conn.commit()
                        pg_cursor.close()
                        
                        processed_count += 1
                        
                        if processed_count % 25 == 0:
                            print(f"Processed {processed_count} records...")
                        
                    except Exception as e:
                        logger.error(f"Error processing record: {e}")
                        error_count += 1
                        pg_conn.rollback()
                        continue
        
        pg_conn.close()
        
        print(f"\nPipeline Summary:")
        print(f"Records processed: {processed_count}")
        print(f"Errors: {error_count}")
        print(f"Success rate: {(processed_count/(processed_count+error_count)*100):.1f}%" if (processed_count+error_count) > 0 else "N/A")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        logger.error(f"Pipeline error: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Simple Corporate Personal Data Pipeline')
    parser.add_argument('--limit', type=int, default=100, help='Number of records to process')
    
    args = parser.parse_args()
    
    success = run_corporate_pipeline(limit=args.limit)
    sys.exit(0 if success else 1)