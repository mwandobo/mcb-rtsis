#!/usr/bin/env python3
"""
Create personalData table in PostgreSQL
Based on personal_data_information-v4.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_personal_data_table():
    """Create the personalDataInformation table in PostgreSQL"""
    
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
        logger.info("Dropping existing personalDataInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "personalDataInformation" CASCADE')
        
        # Create personalDataInformation table
        logger.info("Creating personalDataInformation table...")
        create_table_sql = """
        CREATE TABLE "personalDataInformation" (
            id SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "customerIdentificationNumber" VARCHAR(50),
            "firstName" VARCHAR(255),
            "middleNames" VARCHAR(255),
            "otherNames" VARCHAR(255),
            "fullNames" VARCHAR(500),
            "presentSurname" VARCHAR(255),
            "birthSurname" VARCHAR(255),
            "gender" VARCHAR(50),
            "maritalStatus" VARCHAR(50),
            "numberSpouse" VARCHAR(10),
            "nationality" VARCHAR(100),
            "citizenship" VARCHAR(100),
            "residency" VARCHAR(50),
            "profession" VARCHAR(255),
            "sectorSnaClassification" VARCHAR(100),
            "fateStatus" VARCHAR(50),
            "socialStatus" VARCHAR(50),
            "employmentStatus" VARCHAR(50),
            "monthlyIncome" VARCHAR(100),
            "numberDependants" INTEGER,
            "educationLevel" VARCHAR(100),
            "averageMonthlyExpenditure" VARCHAR(50),
            "negativeClientStatus" VARCHAR(10),
            "spousesFullName" VARCHAR(255),
            "spouseIdentificationType" VARCHAR(100),
            "spouseIdentificationNumber" VARCHAR(50),
            "maidenName" VARCHAR(255),
            "monthlyExpenses" VARCHAR(50),
            "birthDate" DATE,
            "birthCountry" VARCHAR(100),
            "birthPostalCode" VARCHAR(50),
            "birthHouseNumber" VARCHAR(50),
            "birthRegion" VARCHAR(100),
            "birthDistrict" VARCHAR(100),
            "birthWard" VARCHAR(100),
            "birthStreet" VARCHAR(255),
            "identificationType" VARCHAR(100),
            "identificationNumber" VARCHAR(50),
            "issuance_date" DATE,
            "expirationDate" DATE,
            "issuancePlace" VARCHAR(100),
            "issuingAuthority" VARCHAR(255),
            "businessName" VARCHAR(255),
            "establishmentDate" DATE,
            "businessRegistrationNumber" VARCHAR(50),
            "businessRegistrationDate" DATE,
            "businessLicenseNumber" VARCHAR(50),
            "taxIdentificationNumber" VARCHAR(50),
            "employerName" VARCHAR(255),
            "employerRegion" VARCHAR(100),
            "employerDistrict" VARCHAR(100),
            "employerWard" VARCHAR(100),
            "employerStreet" VARCHAR(255),
            "employerHouseNumber" VARCHAR(50),
            "employerPostalCode" VARCHAR(50),
            "businessNature" VARCHAR(255),
            "mobileNumber" VARCHAR(50),
            "alternativeMobileNumber" VARCHAR(50),
            "fixedLineNumber" VARCHAR(50),
            "faxNumber" VARCHAR(50),
            "emailAddress" VARCHAR(255),
            "socialMedia" VARCHAR(255),
            "mainAddress" VARCHAR(255),
            "street" VARCHAR(255),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(50),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "country" VARCHAR(100),
            "sstreet" VARCHAR(255),
            "shouseNumber" VARCHAR(50),
            "spostalCode" VARCHAR(50),
            "sregion" VARCHAR(100),
            "sdistrict" VARCHAR(100),
            "sward" VARCHAR(100),
            "scountry" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_personaldatainformation_reporting_date ON "personalDataInformation"("reportingDate")',
            'CREATE UNIQUE INDEX idx_personaldatainformation_customer_id ON "personalDataInformation"("customerIdentificationNumber")',
            'CREATE INDEX idx_personaldatainformation_first_name ON "personalDataInformation"("firstName")',
            'CREATE INDEX idx_personaldatainformation_full_names ON "personalDataInformation"("fullNames")',
            'CREATE INDEX idx_personaldatainformation_gender ON "personalDataInformation"("gender")',
            'CREATE INDEX idx_personaldatainformation_identification_type ON "personalDataInformation"("identificationType")',
            'CREATE INDEX idx_personaldatainformation_identification_number ON "personalDataInformation"("identificationNumber")',
            'CREATE INDEX idx_personaldatainformation_region ON "personalDataInformation"("region")',
            'CREATE INDEX idx_personaldatainformation_district ON "personalDataInformation"("district")',
            'CREATE INDEX idx_personaldatainformation_mobile_number ON "personalDataInformation"("mobileNumber")',
            'CREATE INDEX idx_personaldatainformation_email_address ON "personalDataInformation"("emailAddress")',
            'CREATE INDEX idx_personaldatainformation_created_at ON "personalDataInformation"(created_at)'
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
            WHERE table_name = 'personalDataInformation'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Personal Data Information table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<40} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<40} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("personalDataInformation table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating personalDataInformation table: {e}")
        raise

if __name__ == "__main__":
    create_personal_data_table()
