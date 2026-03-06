#!/usr/bin/env python3
"""
Create personalDataCorporates table in PostgreSQL
Based on personal-data-corporates-v4.sql structure
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def create_personal_data_corporates_table():
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
        logger.info("Dropping existing personalDataCorporates table if exists...")
        cursor.execute('DROP TABLE IF EXISTS "personalDataCorporates" CASCADE')
        
        logger.info("Creating personalDataCorporates table...")
        create_table_sql = """
        CREATE TABLE "personalDataCorporates" (
            id SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "companyName" VARCHAR(255),
            "customerIdentificationNumber" VARCHAR(50),
            "establishedDate" DATE,
            "legalForm" VARCHAR(100),
            "negativeClientStatus" VARCHAR(50),
            "numberOfEmployees" INTEGER,
            "totalEmployeesMAle" INTEGER,
            "totalEmployeesFemale" INTEGER,
            "registrationCountry" VARCHAR(100),
            "registrationNumber" VARCHAR(50),
            "taxIdentificationNumber" VARCHAR(50),
            "tradeName" VARCHAR(255),
            "parentName" VARCHAR(255),
            "parentIncorporationNumber" VARCHAR(50),
            "groupId" VARCHAR(50),
            "sectorSnaClassification" VARCHAR(100),
            "related_customers" JSONB,
            "street" VARCHAR(255),
            "country" VARCHAR(100),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(50),
            "poBox" VARCHAR(100),
            "zipCode" VARCHAR(50),
            "secondaryStreet" VARCHAR(255),
            "secondartHouseNumber" VARCHAR(50),
            "secondaryPostalCode" VARCHAR(50),
            "secondaryRegion" VARCHAR(100),
            "secondaryDistrict" VARCHAR(100),
            "secondaryCountry" VARCHAR(100),
            "secondaryTextAddress" VARCHAR(255),
            "mobileNumber" VARCHAR(50),
            "alternativeMobileNumber" VARCHAR(50),
            "fixedLineNumber" VARCHAR(50),
            "faxNumber" VARCHAR(50),
            "emailAddress" VARCHAR(255),
            "socialMedia" VARCHAR(255),
            "entityName" VARCHAR(255),
            "entityType" VARCHAR(100),
            "certificateIncorporation" VARCHAR(50),
            "entiryRegion" VARCHAR(100),
            "entityDistrict" VARCHAR(100),
            "entityWard" VARCHAR(100),
            "entityStreet" VARCHAR(255),
            "entityHouseNumber" VARCHAR(50),
            "entityPostalCode" VARCHAR(50),
            "groupParentCode" VARCHAR(50),
            "shareOwnedPercentage" VARCHAR(50),
            "shareOwnedAmount" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        
        logger.info("Creating indexes...")
        indexes = [
            'CREATE INDEX idx_personaldatacorporates_reporting_date ON "personalDataCorporates"("reportingDate")',
            'CREATE UNIQUE INDEX idx_personaldatacorporates_customer_id ON "personalDataCorporates"("customerIdentificationNumber")',
            'CREATE INDEX idx_personaldatacorporates_company_name ON "personalDataCorporates"("companyName")',
            'CREATE INDEX idx_personaldatacorporates_region ON "personalDataCorporates"("region")',
            'CREATE INDEX idx_personaldatacorporates_district ON "personalDataCorporates"("district")',
            'CREATE INDEX idx_personaldatacorporates_created_at ON "personalDataCorporates"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        conn.commit()
        
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'personalDataCorporates'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        logger.info("Personal Data Corporates table created successfully!")
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        logger.info("personalDataCorporates table setup completed!")
        
    except Exception as e:
        logger.error(f"Error creating personalDataCorporates table: {e}")
        raise

if __name__ == "__main__":
    create_personal_data_corporates_table()
