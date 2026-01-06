#!/usr/bin/env python3
"""
Create Personal Data Information Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_personal_data_table():
    """Create the personal data information table in PostgreSQL"""
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
        logger.info("🗑️ Dropping existing personalDataInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "personalDataInformation" CASCADE;')
        
        # Create Personal Data Information table
        logger.info("🏗️ Creating personalDataInformation table...")
        create_table_sql = """
        CREATE TABLE "personalDataInformation" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "customerIdentificationNumber" VARCHAR(50) NOT NULL,
            "firstName" VARCHAR(100),
            "middleNames" VARCHAR(100),
            "otherNames" VARCHAR(100),
            "fullNames" VARCHAR(300),
            "presentSurname" VARCHAR(100),
            "birthSurname" VARCHAR(100),
            "gender" VARCHAR(20),
            "maritalStatus" VARCHAR(50),
            "numberSpouse" VARCHAR(10),
            "spousesFullName" VARCHAR(200),
            "nationality" VARCHAR(100),
            "citizenship" VARCHAR(100),
            "residency" VARCHAR(50),
            "profession" VARCHAR(200),
            "sectorSnaClassification" VARCHAR(200),
            "fateStatus" VARCHAR(50),
            "socialStatus" VARCHAR(50),
            "employmentStatus" VARCHAR(100),
            "monthlyIncome" VARCHAR(50),
            "numberDependants" VARCHAR(10),
            "educationLevel" VARCHAR(100),
            "averageMonthlyExpenditure" VARCHAR(50),
            "monthlyExpenses" VARCHAR(50),
            "negativeClientStatus" VARCHAR(50),
            "spouseIdentificationType" VARCHAR(100),
            "spouseIdentificationNumber" VARCHAR(50),
            "maidenName" VARCHAR(100),
            "birthDate" DATE,
            "birthCountry" VARCHAR(100),
            "birthPostalCode" VARCHAR(20),
            "birthHouseNumber" VARCHAR(20),
            "birthRegion" VARCHAR(100),
            "birthDistrict" VARCHAR(100),
            "birthWard" VARCHAR(100),
            "birthStreet" VARCHAR(200),
            "identificationType" VARCHAR(100),
            "identificationNumber" VARCHAR(50),
            "issuanceDate" DATE,
            "expirationDate" DATE,
            "issuancePlace" VARCHAR(100),
            "issuingAuthority" VARCHAR(100),
            "businessName" VARCHAR(200),
            "establishmentDate" DATE,
            "businessRegistrationNumber" VARCHAR(50),
            "businessRegistrationDate" DATE,
            "businessLicenseNumber" VARCHAR(50),
            "taxIdentificationNumber" VARCHAR(50),
            "employerName" VARCHAR(200),
            "employerRegion" VARCHAR(100),
            "employerDistrict" VARCHAR(100),
            "employerWard" VARCHAR(100),
            "employerStreet" VARCHAR(200),
            "employerHouseNumber" VARCHAR(20),
            "employerPostalCode" VARCHAR(20),
            "businessNature" VARCHAR(200),
            "mobileNumber" VARCHAR(20),
            "alternativeMobileNumber" VARCHAR(20),
            "fixedLineNumber" VARCHAR(20),
            "faxNumber" VARCHAR(20),
            "emailAddress" VARCHAR(100),
            "socialMedia" VARCHAR(200),
            "mainAddress" VARCHAR(500),
            "street" VARCHAR(200),
            "houseNumber" VARCHAR(20),
            "postalCode" VARCHAR(20),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "country" VARCHAR(100),
            "workStreet" VARCHAR(200),
            "workHouseNumber" VARCHAR(20),
            "workPostalCode" VARCHAR(20),
            "workRegion" VARCHAR(100),
            "workDistrict" VARCHAR(100),
            "workWard" VARCHAR(100),
            "workCountry" VARCHAR(100)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_personal_data_unique ON "personalDataInformation"("customerIdentificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_name ON "personalDataInformation"("fullNames");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_first_name ON "personalDataInformation"("firstName");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_surname ON "personalDataInformation"("presentSurname");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_gender ON "personalDataInformation"("gender");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_nationality ON "personalDataInformation"("nationality");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_citizenship ON "personalDataInformation"("citizenship");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_residency ON "personalDataInformation"("residency");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_profession ON "personalDataInformation"("profession");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_employment ON "personalDataInformation"("employmentStatus");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_education ON "personalDataInformation"("educationLevel");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_birth_date ON "personalDataInformation"("birthDate");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_identification ON "personalDataInformation"("identificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_mobile ON "personalDataInformation"("mobileNumber");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_email ON "personalDataInformation"("emailAddress");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_region ON "personalDataInformation"("region");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_district ON "personalDataInformation"("district");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_country ON "personalDataInformation"("country");',
            'CREATE INDEX IF NOT EXISTS idx_personal_data_reporting_date ON "personalDataInformation"("reportingDate");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Personal data information table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'personalDataInformation' 
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
        logger.error(f"❌ Failed to create personal data information table: {e}")
        raise

if __name__ == "__main__":
    create_personal_data_table()