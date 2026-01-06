#!/usr/bin/env python3
"""
Create POS Information Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_pos_table():
    """Create the POS information table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing posInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "posInformation" CASCADE;')
        
        # Create POS Information table
        logger.info("🏗️ Creating posInformation table...")
        create_table_sql = """
        CREATE TABLE "posInformation" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20),
            "posBranchCode" VARCHAR(20),
            "posNumber" VARCHAR(50),
            "qrFsrCode" VARCHAR(50),
            "posHolderName" VARCHAR(200),
            "posHolderNin" VARCHAR(50),
            "posHolderTin" VARCHAR(50),
            "postalCode" VARCHAR(20),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "street" VARCHAR(200),
            "houseNumber" VARCHAR(50),
            "gpsCoordinates" VARCHAR(100),
            "linkedAccount" VARCHAR(50),
            "issueDate" VARCHAR(20),
            "returnDate" VARCHAR(20)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_pos_number_unique ON "posInformation"("posNumber");',
            'CREATE INDEX IF NOT EXISTS idx_pos_qr_fsr_code ON "posInformation"("qrFsrCode");',
            'CREATE INDEX IF NOT EXISTS idx_pos_holder_name ON "posInformation"("posHolderName");',
            'CREATE INDEX IF NOT EXISTS idx_pos_holder_tin ON "posInformation"("posHolderTin");',
            'CREATE INDEX IF NOT EXISTS idx_pos_region ON "posInformation"("region");',
            'CREATE INDEX IF NOT EXISTS idx_pos_district ON "posInformation"("district");',
            'CREATE INDEX IF NOT EXISTS idx_pos_linked_account ON "posInformation"("linkedAccount");',
            'CREATE INDEX IF NOT EXISTS idx_pos_issue_date ON "posInformation"("issueDate");',
            'CREATE INDEX IF NOT EXISTS idx_pos_branch_code ON "posInformation"("posBranchCode");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ POS table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'posInformation' 
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
        logger.error(f"❌ Failed to create POS table: {e}")
        raise

if __name__ == "__main__":
    create_pos_table()