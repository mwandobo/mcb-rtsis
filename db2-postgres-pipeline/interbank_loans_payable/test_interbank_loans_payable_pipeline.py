#!/usr/bin/env python3
"""
Test Interbank Loans Payable Pipeline
Validates the pipeline setup and basic functionality
"""

import sys
import os
import logging
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection
from interbank_loans_payable_streaming_pipeline import InterbankLoansPayableStreamingPipeline
import psycopg2

def test_db2_connection():
    """Test DB2 connection and query"""
    logger = logging.getLogger(__name__)
    logger.info("Testing DB2 connection...")
    
    try:
        db2_conn = DB2Connection()
        
        # Test basic connection
        if not db2_conn.test_connection():
            logger.error("DB2 connection test failed")
            return False
        
        # Test query execution
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM TREASURY_MM_DEAL 
                WHERE DEAL_OPERATION = 'B' AND STATUS != 'C'
            """)
            result = cursor.fetchone()
            count = result[0] if result else 0
            logger.info(f"Found {count:,} interbank loans payable records in DB2")
        
        return True
        
    except Exception as e:
        logger.error(f"DB2 connection test failed: {e}")
        return False

def test_postgresql_connection():
    """Test PostgreSQL connection"""
    logger = logging.getLogger(__name__)
    logger.info("Testing PostgreSQL connection...")
    
    try:
        config = Config()
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        logger.info(f"PostgreSQL connection successful: {version}")
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'interbankLoansPayable'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            cursor.execute('SELECT COUNT(*) FROM "interbankLoansPayable"')
            count = cursor.fetchone()[0]
            logger.info(f"Table 'interbankLoansPayable' exists with {count:,} records")
        else:
            logger.warning("Table 'interbankLoansPayable' does not exist")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {e}")
        return False

def test_rabbitmq_connection():
    """Test RabbitMQ connection"""
    logger = logging.getLogger(__name__)
    logger.info("Testing RabbitMQ connection...")
    
    try:
        pipeline = InterbankLoansPayableStreamingPipeline()
        connection, channel = pipeline.setup_rabbitmq_connection()
        
        # Test queue declaration
        channel.queue_declare(queue='test_interbank_loans_payable', durable=True)
        logger.info("RabbitMQ connection successful")
        
        # Clean up test queue
        channel.queue_delete(queue='test_interbank_loans_payable')
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"RabbitMQ connection test failed: {e}")
        return False

def test_sql_query():
    """Test the SQL query execution"""
    logger = logging.getLogger(__name__)
    logger.info("Testing SQL query execution...")
    
    try:
        pipeline = InterbankLoansPayableStreamingPipeline()
        query = pipeline.get_interbank_loans_payable_query()
        
        # Test query with FETCH FIRST 5 ROWS ONLY
        test_query = query.replace("ORDER BY mm.DEAL_DATE DESC;", "ORDER BY mm.DEAL_DATE DESC FETCH FIRST 5 ROWS ONLY")
        
        with pipeline.db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(test_query)
            rows = cursor.fetchall()
            
            logger.info(f"Query executed successfully, fetched {len(rows)} test records")
            
            if rows:
                # Test record processing
                record = pipeline.process_record(rows[0])
                logger.info(f"Sample record processed: Account Number = {record.accountNumber}, Lender = {record.lenderName}")
                
                # Test validation
                is_valid = pipeline.validate_record(record)
                logger.info(f"Record validation: {'PASSED' if is_valid else 'FAILED'}")
        
        return True
        
    except Exception as e:
        logger.error(f"SQL query test failed: {e}")
        return False

def run_mini_pipeline_test():
    """Run a mini pipeline test with a small batch"""
    logger = logging.getLogger(__name__)
    logger.info("Running mini pipeline test...")
    
    try:
        # Create pipeline with small batch sizes for testing
        pipeline = InterbankLoansPayableStreamingPipeline(batch_size=10, consumer_batch_size=5)
        
        # Setup queue
        pipeline.setup_rabbitmq_queue()
        
        # Test producer for a few records
        logger.info("Testing producer with 10 records...")
        
        query = pipeline.get_interbank_loans_payable_query()
        test_query = query.replace("ORDER BY mm.DEAL_DATE DESC;", "ORDER BY mm.DEAL_DATE DESC FETCH FIRST 10 ROWS ONLY")
        
        connection, channel = pipeline.setup_rabbitmq_connection()
        
        with pipeline.db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(test_query)
            rows = cursor.fetchall()
            
            published_count = 0
            for row in rows:
                record = pipeline.process_record(row)
                if pipeline.validate_record(record):
                    import json
                    from dataclasses import asdict
                    message = json.dumps(asdict(record), default=str)
                    
                    channel.basic_publish(
                        exchange='',
                        routing_key='interbank_loans_payable_queue',
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    published_count += 1
        
        connection.close()
        logger.info(f"Published {published_count} test messages to queue")
        
        # Check queue status
        connection, channel = pipeline.setup_rabbitmq_connection()
        queue_info = channel.queue_declare(queue='interbank_loans_payable_queue', durable=True, passive=True)
        message_count = queue_info.method.message_count
        logger.info(f"Queue now contains {message_count} messages")
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Mini pipeline test failed: {e}")
        return False

def main():
    """Main test function"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("INTERBANK LOANS PAYABLE PIPELINE TEST SUITE")
    logger.info("=" * 60)
    
    tests = [
        ("DB2 Connection", test_db2_connection),
        ("PostgreSQL Connection", test_postgresql_connection),
        ("RabbitMQ Connection", test_rabbitmq_connection),
        ("SQL Query Execution", test_sql_query),
        ("Mini Pipeline Test", run_mini_pipeline_test)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- Running {test_name} Test ---")
        start_time = time.time()
        
        try:
            result = test_func()
            results[test_name] = result
            status = "PASSED" if result else "FAILED"
            elapsed = time.time() - start_time
            logger.info(f"{test_name}: {status} ({elapsed:.2f}s)")
            
        except Exception as e:
            results[test_name] = False
            elapsed = time.time() - start_time
            logger.error(f"{test_name}: FAILED ({elapsed:.2f}s) - {e}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✓ PASSED" if result else "✗ FAILED"
        logger.info(f"{test_name:<25}: {status}")
    
    logger.info("-" * 60)
    logger.info(f"Overall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Pipeline is ready to run.")
        return 0
    else:
        logger.error("❌ Some tests failed. Please fix issues before running pipeline.")
        return 1

if __name__ == "__main__":
    import pika  # Import here to avoid issues if not installed
    sys.exit(main())