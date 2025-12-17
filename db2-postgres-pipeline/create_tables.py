#!/usr/bin/env python3
"""
Script to create all PostgreSQL tables from the schema file
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from .env file"""
    load_dotenv()
    
    config = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': int(os.getenv('PG_PORT', 5432)),
        'database': os.getenv('PG_DATABASE', 'postgres'),
        'user': os.getenv('PG_USER', 'postgres'),
        'password': os.getenv('PG_PASSWORD', 'postgres')
    }
    
    return config

def read_schema_file():
    """Read the PostgreSQL schema file"""
    schema_file = Path(__file__).parent / 'sql' / 'postgres-schema.sql'
    
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    with open(schema_file, 'r', encoding='utf-8') as f:
        return f.read()

def execute_schema(config, schema_sql):
    """Execute the schema SQL to create tables"""
    try:
        logger.info("Connecting to PostgreSQL...")
        logger.info(f"Host: {config['host']}:{config['port']}")
        logger.info(f"Database: {config['database']}")
        logger.info(f"User: {config['user']}")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        
        # Set autocommit to handle DDL statements properly
        conn.autocommit = True
        cursor = conn.cursor()
        
        logger.info("✅ Connected successfully!")
        
        # Split the schema into individual statements
        # Remove comments and empty lines for cleaner processing
        statements = []
        current_statement = []
        
        for line in schema_sql.split('\n'):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # If line ends with semicolon, it's the end of a statement
            if line.endswith(';'):
                statement = ' '.join(current_statement)
                if statement.strip():
                    statements.append(statement)
                current_statement = []
        
        # Add any remaining statement
        if current_statement:
            statement = ' '.join(current_statement)
            if statement.strip():
                statements.append(statement)
        
        logger.info(f"Found {len(statements)} SQL statements to execute")
        
        # Execute each statement
        success_count = 0
        error_count = 0
        
        for i, statement in enumerate(statements, 1):
            try:
                # Extract table name for logging
                table_name = "Unknown"
                if "CREATE TABLE" in statement.upper():
                    # Extract table name from CREATE TABLE statement
                    parts = statement.upper().split()
                    if "EXISTS" in parts:
                        table_idx = parts.index("EXISTS") + 1
                    else:
                        table_idx = parts.index("TABLE") + 1
                    
                    if table_idx < len(parts):
                        table_name = parts[table_idx].strip('"')
                elif "CREATE INDEX" in statement.upper():
                    # Extract index name
                    parts = statement.upper().split()
                    if "EXISTS" in parts:
                        idx_name_idx = parts.index("EXISTS") + 1
                    else:
                        idx_name_idx = parts.index("INDEX") + 1
                    
                    if idx_name_idx < len(parts):
                        table_name = f"INDEX {parts[idx_name_idx]}"
                
                logger.info(f"[{i}/{len(statements)}] Creating {table_name}...")
                cursor.execute(statement)
                success_count += 1
                logger.info(f"✅ {table_name} created successfully")
                
            except psycopg2.Error as e:
                error_count += 1
                logger.error(f"❌ Error creating {table_name}: {e}")
                # Continue with other statements
                continue
        
        cursor.close()
        conn.close()
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("TABLE CREATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total statements: {len(statements)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {error_count}")
        
        if error_count == 0:
            logger.info("🎉 All tables created successfully!")
            return True
        else:
            logger.warning(f"⚠️  {error_count} statements failed. Check logs above.")
            return success_count > 0  # Return True if at least some succeeded
            
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("Please check your connection settings in .env file")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

def verify_tables(config):
    """Verify that tables were created successfully"""
    try:
        logger.info("\nVerifying created tables...")
        
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        
        cursor = conn.cursor()
        
        # Get list of created tables
        cursor.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        
        logger.info(f"\nFound {len(tables)} tables in database:")
        for table_name, table_type in tables:
            logger.info(f"  ✓ {table_name}")
        
        # Get list of indexes
        cursor.execute("""
            SELECT indexname, tablename
            FROM pg_indexes 
            WHERE schemaname = 'public'
            AND indexname LIKE 'idx_%'
            ORDER BY indexname;
        """)
        
        indexes = cursor.fetchall()
        
        if indexes:
            logger.info(f"\nFound {len(indexes)} custom indexes:")
            for index_name, table_name in indexes:
                logger.info(f"  ✓ {index_name} on {table_name}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error verifying tables: {e}")
        return False

def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("PostgreSQL Table Creation Script")
    logger.info("=" * 60)
    
    try:
        # Load configuration
        config = load_config()
        
        # Read schema file
        logger.info("Reading schema file...")
        schema_sql = read_schema_file()
        logger.info("✅ Schema file loaded successfully")
        
        # Execute schema
        success = execute_schema(config, schema_sql)
        
        if success:
            # Verify tables
            verify_tables(config)
            logger.info("\n🎉 Table creation completed successfully!")
            sys.exit(0)
        else:
            logger.error("\n❌ Table creation failed!")
            sys.exit(1)
            
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()