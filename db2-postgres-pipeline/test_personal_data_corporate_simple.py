#!/usr/bin/env python3
"""
Simple test for Personal Data Corporate pipeline
Tests the SQL query and processor without full pipeline
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

def test_corporate_query():
    """Test the corporate SQL query and processor"""
    
    print("🏢 Testing Personal Data Corporate Query")
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
        
        # Add LIMIT for testing
        test_query = f"""
        SELECT * FROM (
            {query}
        ) AS corporate_data
        ORDER BY customerIdentificationNumber
        FETCH FIRST 5 ROWS ONLY
        """
        
        print("📋 Executing test query...")
        logger.info(f"Query: {test_query[:200]}...")
        
        # Connect to DB2
        db2_conn_manager = DB2Connection()
        
        with db2_conn_manager.get_connection() as db2_conn:
            db2_cursor = db2_conn.cursor()
            
            # Execute query
            db2_cursor.execute(test_query)
            rows = db2_cursor.fetchall()
        
        print(f"✅ Query executed successfully! Found {len(rows)} corporate records")
        
        if rows:
            # Test the processor
            processor = PersonalDataCorporateProcessor()
            
            print("\n📊 Processing sample records:")
            for i, row in enumerate(rows, 1):
                try:
                    print(f"\n--- Record {i} ---")
                    print(f"Raw data length: {len(row)} columns")
                    
                    # Process the record
                    record = processor.process_record(row, 'personalDataCorporate')
                    
                    print(f"✅ Processed successfully:")
                    print(f"   Customer ID: {record.customerIdentificationNumber}")
                    print(f"   Company Name: {record.companyName}")
                    print(f"   Trade Name: {record.tradeName}")
                    print(f"   Legal Form: {record.legalForm}")
                    print(f"   Employees: {record.numberOfEmployees}")
                    print(f"   Registration Country: {record.registrationCountry}")
                    print(f"   Tax ID: {record.taxIdentificationNumber}")
                    
                    # Test PostgreSQL insertion
                    config = Config()
                    pg_conn = psycopg2.connect(
                        host=config.database.pg_host,
                        port=config.database.pg_port,
                        database=config.database.pg_database,
                        user=config.database.pg_user,
                        password=config.database.pg_password
                    )
                    
                    pg_cursor = pg_conn.cursor()
                    processor.insert_to_postgres(record, pg_cursor)
                    pg_conn.commit()
                    pg_cursor.close()
                    pg_conn.close()
                    
                    print(f"   💾 Inserted to PostgreSQL successfully!")
                    
                except Exception as e:
                    print(f"   ❌ Error processing record {i}: {e}")
                    logger.error(f"Record processing error: {e}")
                    continue
        
        print(f"\n🎯 Test completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.error(f"Test error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_corporate_query()