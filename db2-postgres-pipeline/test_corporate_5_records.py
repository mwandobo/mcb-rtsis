#!/usr/bin/env python3
"""
Test corporate pipeline with 5 records to debug JSON issue
"""

import logging
from personal_data_corporate_streaming_pipeline import PersonalDataCorporateStreamingPipeline
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_corporate_5_records():
    logger.info("Testing corporate pipeline with 5 records...")
    
    db2_conn = DB2Connection()
    pipeline = PersonalDataCorporateStreamingPipeline(batch_size=5)
    
    try:
        # Test the query
        query = pipeline.get_corporate_query()
        logger.info("Query generated successfully")
        
        # Execute query and check results
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            logger.info(f"Found {len(rows)} records")
            
            if rows:
                # Check the related_customers field (index 17)
                for i, row in enumerate(rows[:2]):  # Check first 2 records
                    logger.info(f"Record {i+1}:")
                    logger.info(f"  Company: {row[1]}")
                    logger.info(f"  Customer ID: {row[2]}")
                    logger.info(f"  Related Customers: {row[17]}")
                    
                    # Process record to see if it works
                    try:
                        record = pipeline.process_record(row)
                        logger.info(f"  Processed successfully: {record.companyName}")
                        logger.info(f"  Related Customers JSON: {record.relatedCustomers}")
                    except Exception as e:
                        logger.error(f"  Processing failed: {e}")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    test_corporate_5_records()