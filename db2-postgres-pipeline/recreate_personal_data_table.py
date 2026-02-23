#!/usr/bin/env python3
"""
Recreate personalData table with updated schema (monthlyIncome as VARCHAR)
"""

import psycopg2
from config import Config

def recreate_personal_data_table():
    """Drop and recreate personalData table"""
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
        
        print("🗑️  Dropping existing personalData table...")
        cursor.execute('DROP TABLE IF EXISTS "personalData" CASCADE')
        conn.commit()
        print("✅ Table dropped")
        
        print("📋 Creating new personalData table with VARCHAR monthlyIncome...")
        
        create_table_query = """
        CREATE TABLE "personalData" (
            "reportingDate" TIMESTAMP,
            "customerIdentificationNumber" VARCHAR(50),
            "firstName" VARCHAR(100),
            "middleNames" VARCHAR(100),
            "otherNames" VARCHAR(100),
            "fullNames" VARCHAR(300),
            "presentSurname" VARCHAR(100),
            "birthSurname" VARCHAR(100),
            "gender" VARCHAR(20),
            "maritalStatus" VARCHAR(50),
            "numberSpouse" VARCHAR(10),
            "nationality" VARCHAR(100),
            "citizenship" VARCHAR(100),
            "residency" VARCHAR(20),
            "profession" VARCHAR(100),
            "sectorSnaClassification" VARCHAR(100),
            "fateStatus" VARCHAR(50),
            "socialStatus" VARCHAR(50),
            "employmentStatus" VARCHAR(50),
            "monthlyIncome" VARCHAR(100),
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
            "birthPostalCode" VARCHAR(50),
            "birthHouseNumber" VARCHAR(50),
            "birthRegion" VARCHAR(100),
            "birthDistrict" VARCHAR(100),
            "birthWard" VARCHAR(100),
            "birthStreet" VARCHAR(200),
            "identificationType" VARCHAR(50),
            "identificationNumber" VARCHAR(50),
            "issuanceDate" VARCHAR(50),
            "expirationDate" VARCHAR(50),
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
            "employerHouseNumber" VARCHAR(50),
            "employerPostalCode" VARCHAR(50),
            "businessNature" VARCHAR(200),
            "mobileNumber" VARCHAR(50),
            "alternativeMobileNumber" VARCHAR(50),
            "fixedLineNumber" VARCHAR(50),
            "faxNumber" VARCHAR(50),
            "emailAddress" VARCHAR(100),
            "socialMedia" VARCHAR(200),
            "mainAddress" VARCHAR(500),
            "street" VARCHAR(200),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(50),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "country" VARCHAR(100),
            "sstreet" VARCHAR(200),
            "shouseNumber" VARCHAR(50),
            "spostalCode" VARCHAR(50),
            "sregion" VARCHAR(100),
            "sdistrict" VARCHAR(100),
            "sward" VARCHAR(100),
            "scountry" VARCHAR(100),
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ Table created successfully")
        
        # Create trigger for updatedAt
        print("🔧 Creating trigger for updatedAt...")
        trigger_function = """
        CREATE OR REPLACE FUNCTION update_personal_data_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW."updatedAt" = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        trigger = """
        CREATE TRIGGER personal_data_updated_at_trigger
        BEFORE UPDATE ON "personalData"
        FOR EACH ROW
        EXECUTE FUNCTION update_personal_data_updated_at();
        """
        
        cursor.execute(trigger_function)
        cursor.execute(trigger)
        conn.commit()
        print("✅ Trigger created")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*60)
        print("✅ personalData table recreated successfully!")
        print("📊 Key change: monthlyIncome is now VARCHAR(100)")
        print("🔄 Ready for v3 pipeline with income level descriptions")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🔄 RECREATING personalData TABLE")
    print("="*60)
    recreate_personal_data_table()
