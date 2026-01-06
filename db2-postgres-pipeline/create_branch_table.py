#!/usr/bin/env python3
"""
Create branch table in PostgreSQL for RTSIS reporting
"""

import psycopg2
from config import Config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_branch_table():
    """Create branch table with proper structure and indexes"""
    
    # Initialize config
    config = Config()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS "branch" (
        "id" SERIAL PRIMARY KEY,
        "reportingDate" TIMESTAMP NOT NULL,
        "branchName" VARCHAR(255) NOT NULL,
        "taxIdentificationNumber" VARCHAR(50),
        "businessLicense" VARCHAR(100),
        "branchCode" VARCHAR(20) NOT NULL UNIQUE,
        "qrFsrCode" VARCHAR(50),
        "region" VARCHAR(100),
        "district" VARCHAR(100),
        "ward" VARCHAR(100),
        "street" VARCHAR(255),
        "houseNumber" VARCHAR(50),
        "postalCode" VARCHAR(20),
        "gpsCoordinates" VARCHAR(100),
        "bankingServices" VARCHAR(100),
        "mobileMoneyServices" VARCHAR(100),
        "registrationDate" DATE,
        "branchStatus" VARCHAR(50) NOT NULL,
        "closureDate" DATE,
        "contactPerson" VARCHAR(255),
        "telephoneNumber" VARCHAR(50),
        "altTelephoneNumber" VARCHAR(50),
        "branchCategory" VARCHAR(100),
        "lastModified" TIMESTAMP,
        "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create indexes for better performance
    create_indexes_sql = [
        'CREATE INDEX IF NOT EXISTS "idx_branch_code" ON "branch" ("branchCode");',
        'CREATE INDEX IF NOT EXISTS "idx_branch_status" ON "branch" ("branchStatus");',
        'CREATE INDEX IF NOT EXISTS "idx_branch_reporting_date" ON "branch" ("reportingDate");',
        'CREATE INDEX IF NOT EXISTS "idx_branch_last_modified" ON "branch" ("lastModified");'
    ]
    
    # Create trigger for updated_at
    create_trigger_sql = """
    CREATE OR REPLACE FUNCTION update_branch_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW."updated_at" = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    
    DROP TRIGGER IF EXISTS "trigger_branch_updated_at" ON "branch";
    CREATE TRIGGER "trigger_branch_updated_at"
        BEFORE UPDATE ON "branch"
        FOR EACH ROW
        EXECUTE FUNCTION update_branch_updated_at();
    """
    
    try:
        # Connect to PostgreSQL
        pg_config = {
            'host': config.database.pg_host,
            'port': config.database.pg_port,
            'database': config.database.pg_database,
            'user': config.database.pg_user,
            'password': config.database.pg_password
        }
        conn = psycopg2.connect(**pg_config)
        cursor = conn.cursor()
        
        logger.info("🏢 Creating branch table...")
        
        # Create table
        cursor.execute(create_table_sql)
        logger.info("✅ Branch table created successfully")
        
        # Create indexes
        for index_sql in create_indexes_sql:
            cursor.execute(index_sql)
        logger.info("✅ Branch table indexes created successfully")
        
        # Create trigger
        cursor.execute(create_trigger_sql)
        logger.info("✅ Branch table trigger created successfully")
        
        # Commit changes
        conn.commit()
        
        # Verify table creation
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'branch'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info(f"✅ Branch table created with {len(columns)} columns:")
        for col_name, data_type, nullable in columns:
            logger.info(f"   - {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
        
        cursor.close()
        conn.close()
        
        logger.info("🎉 Branch table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error creating branch table: {str(e)}")
        raise

if __name__ == "__main__":
    create_branch_table()