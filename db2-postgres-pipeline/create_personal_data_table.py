#!/usr/bin/env python3
"""
Create personalData table with camelCase naming
"""

import psycopg2
from config import Config

def create_personal_data_table():
    """Create personalData table in PostgreSQL with camelCase naming"""
    
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
        cursor.execute('DROP TABLE IF EXISTS "personalData"')
        
        # Create personalData table with camelCase fields
        create_table_sql = """
        CREATE TABLE "personalData" (
            id SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "customerIdentificationNumber" VARCHAR(50),
            "firstName" VARCHAR(100),
            "middleNames" VARCHAR(100),
            "otherNames" VARCHAR(100),
            "fullNames" VARCHAR(300),
            "presentSurname" VARCHAR(100),
            "birthSurname" VARCHAR(100),
            "gender" VARCHAR(20),
            "maritalStatus" VARCHAR(20),
            "numberSpouse" VARCHAR(10),
            "nationality" VARCHAR(100),
            "citizenship" VARCHAR(100),
            "residency" VARCHAR(20),
            "profession" VARCHAR(100),
            "sectorSnaClassification" VARCHAR(100),
            "fateStatus" VARCHAR(50),
            "socialStatus" VARCHAR(50),
            "employmentStatus" VARCHAR(50),
            "monthlyIncome" DECIMAL(18,2),
            "numberDependants" INTEGER,
            "educationLevel" VARCHAR(100),
            "averageMonthlyExpenditure" DECIMAL(18,2),
            "negativeClientStatus" VARCHAR(10),
            "spousesFullName" VARCHAR(200),
            "spouseIdentificationType" VARCHAR(50),
            "spouseIdentificationNumber" VARCHAR(50),
            "maidenName" VARCHAR(100),
            "monthlyExpenses" DECIMAL(18,2),
            "birthDate" DATE,
            "birthCountry" VARCHAR(100),
            "birthPostalCode" VARCHAR(20),
            "birthHouseNumber" VARCHAR(20),
            "birthRegion" VARCHAR(100),
            "birthDistrict" VARCHAR(100),
            "birthWard" VARCHAR(100),
            "birthStreet" VARCHAR(200),
            "identificationType" VARCHAR(50),
            "identificationNumber" VARCHAR(50),
            "issuanceDate" VARCHAR(20),
            "expirationDate" VARCHAR(20),
            "issuancePlace" VARCHAR(100),
            "issuingAuthority" VARCHAR(200),
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
            "sstreet" VARCHAR(200),
            "shouseNumber" VARCHAR(20),
            "spostalCode" VARCHAR(20),
            "sregion" VARCHAR(100),
            "sdistrict" VARCHAR(100),
            "sward" VARCHAR(100),
            "scountry" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalData_customerIdentificationNumber" ON "personalData" ("customerIdentificationNumber")')
        cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalData_reportingDate" ON "personalData" ("reportingDate")')
        cursor.execute('CREATE INDEX IF NOT EXISTS "idx_personalData_identificationNumber" ON "personalData" ("identificationNumber")')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ personalData table created successfully with camelCase naming")
        print("📋 Table structure:")
        print("   - Table name: personalData (camelCase)")
        print("   - 73 fields with camelCase naming")
        print("   - Indexes on customerIdentificationNumber, reportingDate, identificationNumber")
        
    except Exception as e:
        print(f"❌ Error creating personalData table: {e}")
        raise

if __name__ == "__main__":
    create_personal_data_table()