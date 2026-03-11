#!/usr/bin/env python3
"""
Test Channel Record Information Pipeline
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
from channel_record_information_streaming_pipeline import ChannelRecordInformationStreamingPipeline
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
        
        # Test query execution - adjust table name based on actual source
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            # Generic test query - adjust based on actual data source
            cursor.execute("SELECT COUNT(*) FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()
            count = result[0] if result else 0
            logger.info(f"DB2 connection test successful, basic query returned: {count}")
        
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
                WHERE table_name = 'channelRecordInformation'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            cursor.execute('SELECT COUNT(*) FROM "channelRecordInformation"')
            count = cursor.fetchone()[0]
            logger.info(f"Table 'channelRecordInformation' exists with {count:,} records")
        else:
            logger.warning("Table 'channelRecordInformation' does not exist")
        
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
        pipeline = ChannelRecordInformationStreamingPipeline()
        connection, channel = pipeline.setup_rabbitmq_connection()
        
        # Test queue declaration
        channel.queue_declare(queue='test_channel_record_information', durable=True)
        logger.info("RabbitMQ connection successful")
        
        # Clean up test queue
        channel.queue_delete(queue='test_channel_record_information')
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
        pipeline = ChannelRecordInformationStreamingPipeline()
        
        # Test if pipeline has the query method
        if hasattr(pipeline, 'get_channel_record_information_query'):
            query = pipeline.get_channel_record_information_query()
            logger.info("SQL query loaded successfully")
            logger.info(f"Query length: {len(query)} characters")
        else:
            logger.warning("Pipeline doesn't have get_channel_record_information_query method")
            return False
        
        # Test basic DB2 query execution
        with pipeline.db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            # Test with a simple query first
            cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            result = cursor.fetchone()
            logger.info(f"Basic query test successful: {result[0]}")
        
        return True
        
    except Exception as e:
        logger.error(f"SQL query test failed: {e}")
        return False

def test_pipeline_initialization():
    """Test pipeline initialization"""
    logger = logging.getLogger(__name__)
    logger.info("Testing pipeline initialization...")
    
    try:
        # Test pipeline creation with default settings
        pipeline = ChannelRecordInformationStreamingPipeline(batch_size=10, consumer_batch_size=5)
        
        logger.info(f"Pipeline initialized successfully")
        logger.info(f"Batch size: {pipeline.batch_size}")
        logger.info(f"Consumer batch size: {pipeline.consumer_batch_size}")
        
        # Test configuration access
        if hasattr(pipeline, 'config'):
            logger.info("Configuration loaded successfully")
        
        # Test DB2 connection access
        if hasattr(pipeline, 'db2_conn'):
            logger.info("DB2 connection initialized successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline initialization test failed: {e}")
        return False

def run_mini_pipeline_test():
    """Run a mini pipeline test with queue setup"""
    logger = logging.getLogger(__name__)
    logger.info("Running mini pipeline test...")
    
    try:
        # Create pipeline with small batch sizes for testing
        pipeline = ChannelRecordInformationStreamingPipeline(batch_size=5, consumer_batch_size=3)
        
        # Test queue setup
        logger.info("Testing queue setup...")
        pipeline.setup_rabbitmq_queue()
        logger.info("Queue setup successful")
        
        # Test RabbitMQ connection and basic message handling
        connection, channel = pipeline.setup_rabbitmq_connection()
        
        # Check queue status
        queue_info = channel.queue_declare(queue='channel_record_information_queue', durable=True, passive=True)
        message_count = queue_info.method.message_count
        logger.info(f"Queue contains {message_count} messages")
        
        connection.close()
        logger.info("Mini pipeline test completed successfully")
        
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
    logger.info("CHANNEL RECORD INFORMATION PIPELINE TEST SUITE")
    logger.info("=" * 60)
    
    tests = [
        ("DB2 Connection", test_db2_connection),
        ("PostgreSQL Connection", test_postgresql_connection),
        ("RabbitMQ Connection", test_rabbitmq_connection),
        ("Pipeline Initialization", test_pipeline_initialization),
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
    try:
        import pika  # Import here to avoid issues if not installed
        sys.exit(main())
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Please install: pip install pika psycopg2-binary pyodbc")
        sys.exit(1)