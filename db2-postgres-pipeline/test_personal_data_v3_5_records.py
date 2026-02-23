#!/usr/bin/env python3
"""
Test personal data pipeline with v3 SQL - 5 records
"""

import logging
from personal_data_streaming_pipeline import PersonalDataStreamingPipeline
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_personal_data_v3():
    logger.info("Testing personal data pipeline with v3 SQL - 5 records...")
    
    db2_conn = DB2Connection()
    pipeline = PersonalDataStreamingPipeline(batch_size=5)
    
    try:
        # Test the query
        query = pipeline.get_personal_data_query()
        logger.info("V3 query generated successfully")
        
        # Execute query and check results
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            logger.info(f"Found {len(rows)} records")
            
            if rows:
                # Check first few records
                for i, row in enumerate(rows[:2]):
                    logger.info(f"Record {i+1}:")
                    logger.info(f"  Customer ID: {row[1]}")  # customerIdentificationNumber
                    logger.info(f"  Full Name: {row[5]}")    # fullNames
                    logger.info(f"  Gender: {row[8]}")       # gender
                    logger.info(f"  Region: {row[46]}")      # region
                    logger.info(f"  District: {row[47]}")    # district
                    logger.info(f"  Ward: {row[48]}")        # ward
                    
                    # Test processing
                    try:
                        record = pipeline.personal_data_processor.process_record(row)
                        logger.info(f"  ✓ Processed successfully: {record.fullNames}")
                    except Exception as e:
                        logger.error(f"  ✗ Processing failed: {e}")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    test_personal_data_v3()