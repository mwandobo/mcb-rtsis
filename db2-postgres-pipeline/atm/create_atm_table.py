#!/usr/bin/env python3
"""
Create atm table in PostgreSQL
Based on atm.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_atm_table():
    """Create the atm table in PostgreSQL"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Drop table if exists
        logger.info("Dropping existing atm table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS atm CASCADE')
        
        # Create atm table
        logger.info("Creating atm table...")
        create_table_sql = """
        CREATE TABLE atm (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "atmName" VARCHAR(255),
            "branchCode" VARCHAR(50),
            "atmCode" VARCHAR(50),
            "tillNumber" VARCHAR(50),
            "mobileMoneyServices" VARCHAR(100),
            "qrFsrCode" VARCHAR(100),
            "postalCode" VARCHAR(20),
            region VARCHAR(100),
            district VARCHAR(100),
            ward VARCHAR(100),
            street VARCHAR(255),
            "houseNumber" VARCHAR(50),
            "gpsCoordinates" VARCHAR(100),
            "linkedAccount" VARCHAR(50),
            "openingDate" VARCHAR(12),
            "atmStatus" VARCHAR(50),
            "closureDate" VARCHAR(12),
            "atmCategory" VARCHAR(50),
            "atmChannel" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_atm_reporting_date ON atm("reportingDate")',
            'CREATE UNIQUE INDEX idx_atm_code ON atm("atmCode")',
            'CREATE INDEX idx_atm_name ON atm("atmName")',
            'CREATE INDEX idx_atm_branch_code ON atm("branchCode")',
            'CREATE INDEX idx_atm_status ON atm("atmStatus")',
            'CREATE INDEX idx_atm_region ON atm(region)',
            'CREATE INDEX idx_atm_district ON atm(district)',
            'CREATE INDEX idx_atm_created_at ON atm(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            # Extract index name from SQL
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        # Commit changes
        conn.commit()
        
        # Get table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'atm'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("ATM table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<35} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<35} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("ATM table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating atm table: {e}")
        raise

if __name__ == "__main__":
    create_atm_table()
