#!/usr/bin/env python3
"""
Create personalDataCorporate table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_personal_data_corporate_table():
    config = Config()
    
    logger.info("="*80)
    logger.info("CREATING PERSONAL DATA CORPORATE TABLE")
    logger.info("="*80)
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Drop table if exists
        logger.info("Dropping existing table if exists...")
        cursor.execute('DROP TABLE IF EXISTS "personalDataCorporate" CASCADE')
        
        # Create table
        logger.info("Creating personalDataCorporate table...")
        create_table_sql = """
        CREATE TABLE "personalDataCorporate" (
            "reportingDate" TIMESTAMP,
            "companyName" VARCHAR(500),
            "customerIdentificationNumber" VARCHAR(50) PRIMARY KEY,
            "establishedDate" VARCHAR(50),
            "legalForm" VARCHAR(100),
            "negativeClientStatus" VARCHAR(100),
            "registrationCountry" VARCHAR(100),
            "registrationNumber" VARCHAR(100),
            "taxIdentificationNumber" VARCHAR(100),
            "tradeName" VARCHAR(500),
            "parentName" VARCHAR(500),
            "parentIncorporationNumber" VARCHAR(100),
            "groupId" VARCHAR(100),
            "sectorSnaClassification" VARCHAR(200),
            "relatedCustomers" TEXT,
            "entityName" VARCHAR(500),
            "certificateIncorporation" VARCHAR(200),
            "entityRegion" VARCHAR(200),
            "entityDistrict" VARCHAR(200),
            "entityWard" VARCHAR(200),
            "entityStreet" VARCHAR(200),
            "entityHouseNumber" VARCHAR(100),
            "entityPostalCode" VARCHAR(50),
            "groupParentCode" VARCHAR(100),
            "shareOwnedPercentage" VARCHAR(50),
            "shareOwnedAmount" VARCHAR(50),
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        
        # Create trigger function for updatedAt (camelCase)
        logger.info("Creating trigger function...")
        cursor.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column_camelcase()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW."updatedAt" = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)
        
        # Create trigger
        logger.info("Creating trigger...")
        cursor.execute("""
        CREATE TRIGGER update_personal_data_corporate_updated_at
        BEFORE UPDATE ON "personalDataCorporate"
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column_camelcase();
        """)
        
        # Create index
        logger.info("Creating index...")
        cursor.execute("""
        CREATE INDEX idx_personal_data_corporate_customer_id 
        ON "personalDataCorporate"("customerIdentificationNumber")
        """)
        
        conn.commit()
        
        logger.info("✓ Table created successfully")
        logger.info("✓ Trigger created successfully")
        logger.info("✓ Index created successfully")
        logger.info("="*80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    create_personal_data_corporate_table()
