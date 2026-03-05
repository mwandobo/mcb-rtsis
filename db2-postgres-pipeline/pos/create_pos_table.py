#!/usr/bin/env python3
"""
Create pos table in PostgreSQL
Based on pos-v3.sql structure
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_pos_table():
    """Create the pos table in PostgreSQL"""
    
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
        
        logger.info("Dropping existing posInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "posInformation" CASCADE')
        
        logger.info("Creating posInformation table...")
        create_table_sql = """
        CREATE TABLE "posInformation" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "posBranchCode" VARCHAR(50),
            "posNumber" VARCHAR(50),
            "qrFsrCode" VARCHAR(100),
            "posHolderCategory" VARCHAR(100),
            "posHolderName" VARCHAR(255),
            "posHolderNin" VARCHAR(50),
            "posHolderTin" VARCHAR(50),
            "postalCode" VARCHAR(20),
            region VARCHAR(100),
            district VARCHAR(100),
            ward VARCHAR(100),
            street VARCHAR(255),
            "houseNumber" VARCHAR(50),
            "gpsCoordinates" VARCHAR(100),
            "linkedAccount" VARCHAR(50),
            "issueDate" VARCHAR(12),
            "returnDate" VARCHAR(12),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_posInformation_reporting_date ON "posInformation"("reportingDate")',
            'CREATE UNIQUE INDEX idx_posInformation_number ON "posInformation"("posNumber")',
            'CREATE INDEX idx_posInformation_holder_name ON "posInformation"("posHolderName")',
            'CREATE INDEX idx_posInformation_branch_code ON "posInformation"("posBranchCode")',
            'CREATE INDEX idx_posInformation_region ON "posInformation"(region)',
            'CREATE INDEX idx_posInformation_district ON "posInformation"(district)',
            'CREATE INDEX idx_posInformation_created_at ON "posInformation"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        conn.commit()
        
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'posInformation'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("POS Information table created successfully!")
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
        
        logger.info("POS Information table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating pos table: {e}")
        raise

if __name__ == "__main__":
    create_pos_table()
