#!/usr/bin/env python3
"""
PostgreSQL Connection Test Script
Tests connection to PostgreSQL database using configuration from .env file
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from .env file"""
    # Load .env file
    load_dotenv()
    
    config = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': int(os.getenv('PG_PORT', 5432)),
        'database': os.getenv('PG_DATABASE', 'postgres'),
        'user': os.getenv('PG_USER', 'postgres'),
        'password': os.getenv('PG_PASSWORD', 'postgres')
    }
    
    return config

def test_basic_connection(config):
    """Test basic PostgreSQL connection"""
    try:
        logger.info("Testing basic PostgreSQL connection...")
        logger.info(f"Connecting to: {config['host']}:{config['port']}/{config['database']} as {config['user']}")
        
        # Create connection
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            connect_timeout=10
        )
        
        # Test connection
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        logger.info("✅ Connection successful!")
        logger.info(f"PostgreSQL version: {version}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Connection failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

def test_database_info(config):
    """Get database information"""
    try:
        logger.info("\nGetting database information...")
        
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        
        cursor = conn.cursor()
        
        # Get database size
        cursor.execute("""
            SELECT pg_size_pretty(pg_database_size(current_database())) as db_size;
        """)
        db_size = cursor.fetchone()[0]
        logger.info(f"Database size: {db_size}")
        
        # Get current database and user
        cursor.execute("SELECT current_database(), current_user;")
        db_name, current_user = cursor.fetchone()
        logger.info(f"Current database: {db_name}")
        logger.info(f"Current user: {current_user}")
        
        # Get server time
        cursor.execute("SELECT NOW();")
        server_time = cursor.fetchone()[0]
        logger.info(f"Server time: {server_time}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error getting database info: {e}")
        return False

def test_table_access(config):
    """Test access to key tables"""
    try:
        logger.info("\nTesting table access...")
        
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        
        cursor = conn.cursor()
        
        # List of tables to check
        tables_to_check = [
            'cash_information',
            'loan_information', 
            'overdraft',
            'other_assets',
            'investment_debt_securities'
        ]
        
        for table_name in tables_to_check:
            try:
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    # Get row count
                    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(table_name)
                    ))
                    row_count = cursor.fetchone()[0]
                    logger.info(f"✅ Table '{table_name}': {row_count:,} rows")
                else:
                    logger.warning(f"⚠️  Table '{table_name}': Does not exist")
                    
            except Exception as e:
                logger.error(f"❌ Error checking table '{table_name}': {e}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error testing table access: {e}")
        return False

def test_write_permissions(config):
    """Test write permissions by creating and dropping a test table"""
    try:
        logger.info("\nTesting write permissions...")
        
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        
        cursor = conn.cursor()
        
        # Create test table
        test_table_name = f"connection_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        cursor.execute(f"""
            CREATE TABLE {test_table_name} (
                id SERIAL PRIMARY KEY,
                test_data VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        # Insert test data
        cursor.execute(f"""
            INSERT INTO {test_table_name} (test_data) 
            VALUES ('Connection test successful');
        """)
        
        # Read test data
        cursor.execute(f"SELECT test_data FROM {test_table_name};")
        result = cursor.fetchone()[0]
        
        # Drop test table
        cursor.execute(f"DROP TABLE {test_table_name};")
        
        # Commit changes
        conn.commit()
        
        logger.info(f"✅ Write permissions OK - Test data: '{result}'")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Write permission test failed: {e}")
        return False

def main():
    """Main function to run all connection tests"""
    logger.info("=" * 60)
    logger.info("PostgreSQL Connection Test")
    logger.info("=" * 60)
    
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        logger.error(f"❌ Failed to load configuration: {e}")
        sys.exit(1)
    
    # Run tests
    tests_passed = 0
    total_tests = 4
    
    # Test 1: Basic connection
    if test_basic_connection(config):
        tests_passed += 1
    
    # Test 2: Database info
    if test_database_info(config):
        tests_passed += 1
    
    # Test 3: Table access
    if test_table_access(config):
        tests_passed += 1
    
    # Test 4: Write permissions
    if test_write_permissions(config):
        tests_passed += 1
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        logger.info("🎉 All tests passed! PostgreSQL connection is working properly.")
        sys.exit(0)
    else:
        logger.error(f"❌ {total_tests - tests_passed} test(s) failed. Please check your configuration.")
        sys.exit(1)

if __name__ == "__main__":
    main()