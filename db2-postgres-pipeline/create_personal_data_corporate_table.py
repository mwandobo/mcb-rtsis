#!/usr/bin/env python3
"""
Create Personal Data Corporate table in PostgreSQL with camelCase naming
"""

import psycopg2
from config import Config

def create_personal_data_corporate_table():
    """Create the personalDataCorporate table with camelCase naming"""
    
    # Get configuration
    config = Config()
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    pg_cursor = pg_conn.cursor()
    
    try:
        # Drop table if exists
        print("Dropping existing personalDataCorporate table if it exists...")
        pg_cursor.execute('DROP TABLE IF EXISTS "personalDataCorporate"')
        
        # Create table with camelCase naming
        create_table_sql = '''
        CREATE TABLE "personalDataCorporate" (
            id SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "companyName" VARCHAR(255),
            "customerIdentificationNumber" VARCHAR(50),
            "establishmentDate" DATE,
            "legalForm" VARCHAR(100),
            "negativeClientStatus" VARCHAR(100),
            "numberOfEmployees" INTEGER,
            "numberOfEmployeesMale" INTEGER,
            "numberOfEmployeesFemale" INTEGER,
            "registrationCountry" VARCHAR(100),
            "registrationNumber" VARCHAR(100),
            "taxIdentificationNumber" VARCHAR(100),
            "tradeName" VARCHAR(255),
            "parentName" VARCHAR(255),
            "parentIncorporationNumber" VARCHAR(100),
            "groupId" VARCHAR(100),
            "sectorSnaClassification" VARCHAR(100),
            "fullName" VARCHAR(255),
            "gender" VARCHAR(50),
            "cellPhone" VARCHAR(50),
            "relationType" VARCHAR(100),
            "nationalId" VARCHAR(100),
            "appointmentDate" VARCHAR(50),
            "terminationDate" VARCHAR(50),
            "rateValueOfSharesOwned" VARCHAR(100),
            "amountValueOfSharesOwned" VARCHAR(100),
            "street" VARCHAR(255),
            "country" VARCHAR(100),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(50),
            "poBox" VARCHAR(50),
            "zipCode" VARCHAR(50),
            "primaryPostalCode" VARCHAR(50),
            "primaryRegion" VARCHAR(100),
            "primaryDistrict" VARCHAR(100),
            "primaryWard" VARCHAR(100),
            "primaryStreet" VARCHAR(255),
            "primaryHouseNumber" VARCHAR(50),
            "secondaryStreet" VARCHAR(255),
            "secondaryHouseNumber" VARCHAR(50),
            "secondaryPostalCode" VARCHAR(50),
            "secondaryRegion" VARCHAR(100),
            "secondaryDistrict" VARCHAR(100),
            "secondaryCountry" VARCHAR(100),
            "secondaryTextAddress" TEXT,
            "mobileNumber" VARCHAR(50),
            "alternativeMobileNumber" VARCHAR(50),
            "fixedLineNumber" VARCHAR(50),
            "faxNumber" VARCHAR(50),
            "emailAddress" VARCHAR(255),
            "socialMedia" VARCHAR(255),
            "entityName" VARCHAR(255),
            "entityType" VARCHAR(100),
            "certificateIncorporation" VARCHAR(100),
            "entityRegion" VARCHAR(100),
            "entityDistrict" VARCHAR(100),
            "entityWard" VARCHAR(100),
            "entityStreet" VARCHAR(255),
            "entityHouseNumber" VARCHAR(50),
            "entityPostalCode" VARCHAR(50),
            "groupParentCode" VARCHAR(100),
            "shareOwnedPercentage" VARCHAR(100),
            "shareOwnedAmount" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
        
        print("Creating personalDataCorporate table...")
        pg_cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        print("Creating indexes...")
        
        # Index on customer identification number for fast lookups
        pg_cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalDataCorporate_customerIdentificationNumber" ON "personalDataCorporate" ("customerIdentificationNumber")')
        
        # Index on company name for searches
        pg_cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalDataCorporate_companyName" ON "personalDataCorporate" ("companyName")')
        
        # Index on reporting date for time-based queries
        pg_cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalDataCorporate_reportingDate" ON "personalDataCorporate" ("reportingDate")')
        
        # Index on registration number
        pg_cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalDataCorporate_registrationNumber" ON "personalDataCorporate" ("registrationNumber")')
        
        # Index on tax identification number
        pg_cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalDataCorporate_taxIdentificationNumber" ON "personalDataCorporate" ("taxIdentificationNumber")')
        
        # Commit the changes
        pg_conn.commit()
        print("✅ Personal Data Corporate table created successfully with camelCase naming!")
        
        # Show table info
        pg_cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'personalDataCorporate' 
            ORDER BY ordinal_position
        """)
        
        columns = pg_cursor.fetchall()
        print(f"\n📊 Table structure (Total columns: {len(columns)}):")
        for i, (col_name, data_type, nullable) in enumerate(columns, 1):
            print(f"{i:2d}. {col_name:<35} {data_type:<20} {'NULL' if nullable == 'YES' else 'NOT NULL'}")
            
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        pg_conn.rollback()
        raise
    finally:
        pg_cursor.close()
        pg_conn.close()

if __name__ == "__main__":
    create_personal_data_corporate_table()