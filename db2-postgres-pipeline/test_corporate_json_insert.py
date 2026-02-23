#!/usr/bin/env python3
"""
Test corporate pipeline with JSON insert to PostgreSQL
"""

import logging
import json
from personal_data_corporate_streaming_pipeline import PersonalDataCorporateStreamingPipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_json_insert():
    logger.info("Testing corporate JSON insert to PostgreSQL...")
    
    pipeline = PersonalDataCorporateStreamingPipeline(batch_size=2)
    
    try:
        # Test with 2 records
        with pipeline.db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            query = pipeline.get_corporate_query()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            logger.info(f"Found {len(rows)} records")
            
            if rows:
                for i, row in enumerate(rows[:2]):
                    logger.info(f"\nTesting record {i+1}:")
                    logger.info(f"  Company: {row[1]}")
                    
                    # Process record
                    record = pipeline.process_record(row)
                    
                    # Validate JSON
                    try:
                        json_data = json.loads(record.relatedCustomers)
                        logger.info(f"  ✓ Valid JSON with {len(json_data)} related customers")
                        
                        if json_data:
                            logger.info(f"  First customer: {json_data[0]['fullName']}")
                    except json.JSONDecodeError as e:
                        logger.error(f"  ✗ Invalid JSON: {e}")
                        continue
                    
                    # Test PostgreSQL insert
                    try:
                        with pipeline.get_postgres_connection() as pg_conn:
                            pg_cursor = pg_conn.cursor()
                            pipeline.insert_to_postgres(record, pg_cursor)
                            pg_conn.commit()
                            logger.info(f"  ✓ Successfully inserted to PostgreSQL")
                    except Exception as e:
                        logger.error(f"  ✗ PostgreSQL insert failed: {e}")
                        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    test_json_insert()