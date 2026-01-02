#!/usr/bin/env python3
"""
Create ATM Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_atm_table():
    """Create the ATM information table in PostgreSQL"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        logger.info("🗑️ Dropping existing atmInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "atmInformation" CASCADE;')
        
        # Create ATM Information table
        logger.info("🏗️ Creating atmInformation table...")
        create_table_sql = """
        CREATE TABLE "atmInformation" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20),
            "atmName" VARCHAR(200),
            "branchCode" VARCHAR(20),
            "atmCode" VARCHAR(50),
            "tillNumber" VARCHAR(50),
            "mobileMoneyServices" VARCHAR(100),
            "qrFsrCode" VARCHAR(50),
            "postalCode" VARCHAR(20),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "street" VARCHAR(200),
            "houseNumber" VARCHAR(50),
            "gpsCoordinates" VARCHAR(100),
            "linkedAccount" VARCHAR(50),
            "openingDate" VARCHAR(20),
            "atmStatus" VARCHAR(50),
            "closureDate" VARCHAR(20),
            "atmCategory" VARCHAR(50),
            "atmChannel" VARCHAR(100)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_atm_code_unique ON "atmInformation"("atmCode");',
            'CREATE INDEX IF NOT EXISTS idx_atm_branch_code ON "atmInformation"("branchCode");',
            'CREATE INDEX IF NOT EXISTS idx_atm_status ON "atmInformation"("atmStatus");',
            'CREATE INDEX IF NOT EXISTS idx_atm_category ON "atmInformation"("atmCategory");',
            'CREATE INDEX IF NOT EXISTS idx_atm_region ON "atmInformation"("region");',
            'CREATE INDEX IF NOT EXISTS idx_atm_district ON "atmInformation"("district");',
            'CREATE INDEX IF NOT EXISTS idx_atm_opening_date ON "atmInformation"("openingDate");',
            'CREATE INDEX IF NOT EXISTS idx_atm_linked_account ON "atmInformation"("linkedAccount");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ ATM table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'atmInformation' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for col_name, data_type, max_length in columns:
            length_info = f"({max_length})" if max_length else ""
            logger.info(f"  {col_name}: {data_type}{length_info}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to create ATM table: {e}")
        raise

if __name__ == "__main__":
    create_atm_table()