#!/usr/bin/env python3
"""
Create branch table in PostgreSQL
Based on branch.sql structure
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_branch_table():
    """Create the branch table in PostgreSQL"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        logger.info("Dropping existing branch table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS branch CASCADE')
        
        logger.info("Creating branch table...")
        create_table_sql = """
        CREATE TABLE branch (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "branchName" VARCHAR(255),
            "taxIdentificationNumber" VARCHAR(50),
            "businessLicense" VARCHAR(50),
            "branchCode" VARCHAR(50),
            "qrFsrCode" VARCHAR(100),
            region VARCHAR(100),
            district VARCHAR(100),
            ward VARCHAR(100),
            street VARCHAR(255),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(20),
            "gpsCoordinates" VARCHAR(100),
            "bankingServices" VARCHAR(100),
            "mobileMoneyServices" VARCHAR(100),
            "registrationDate" VARCHAR(12),
            "branchStatus" VARCHAR(50),
            "closureDate" VARCHAR(12),
            "contactPerson" VARCHAR(255),
            "telephoneNumber" VARCHAR(50),
            "altTelephoneNumber" VARCHAR(50),
            "branchCategory" VARCHAR(50),
            "lastModified" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_branch_reporting_date ON branch("reportingDate")',
            'CREATE UNIQUE INDEX idx_branch_code ON branch("branchCode")',
            'CREATE INDEX idx_branch_name ON branch("branchName")',
            'CREATE INDEX idx_branch_status ON branch("branchStatus")',
            'CREATE INDEX idx_branch_region ON branch(region)',
            'CREATE INDEX idx_branch_district ON branch(district)',
            'CREATE INDEX idx_branch_created_at ON branch(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        conn.commit()
        
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'branch'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Branch table created successfully!")
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
        
        logger.info("Branch table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating branch table: {e}")
        raise

if __name__ == "__main__":
    create_branch_table()
