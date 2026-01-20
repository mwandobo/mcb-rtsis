#!/usr/bin/env python3
"""
Create posInformation table with camelCase naming
"""

import psycopg2
from config import Config

def create_pos_information_table():
    """Create the posInformation table with camelCase fields"""
    
    config = Config()
    
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
    drop_query = 'DROP TABLE IF EXISTS "posInformation"'
    cursor.execute(drop_query)
    print("✅ Dropped existing posInformation table (if it existed)")
    
    # Create table with camelCase naming
    create_query = """
    CREATE TABLE "posInformation" (
        "reportingDate" VARCHAR(20) NOT NULL,
        "posBranchCode" INTEGER NOT NULL,
        "posNumber" VARCHAR(50) NOT NULL PRIMARY KEY,
        "qrFsrCode" VARCHAR(50) NOT NULL,
        "posHolderCategory" VARCHAR(50) NOT NULL,
        "posHolderName" VARCHAR(100) NOT NULL,
        "posHolderNin" VARCHAR(50),
        "posHolderTin" VARCHAR(50) NOT NULL,
        "postalCode" VARCHAR(20),
        "region" VARCHAR(100) NOT NULL,
        "district" VARCHAR(100) NOT NULL,
        "ward" VARCHAR(100) NOT NULL,
        "street" VARCHAR(100) NOT NULL,
        "houseNumber" VARCHAR(50) NOT NULL,
        "gpsCoordinates" TEXT,
        "linkedAccount" VARCHAR(50) NOT NULL,
        "issueDate" VARCHAR(20) NOT NULL,
        "returnDate" VARCHAR(20),
        "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(create_query)
    print("✅ Created posInformation table with camelCase fields")
    
    # Create indexes for better performance
    indexes = [
        'CREATE INDEX "idx_posInformation_posBranchCode" ON "posInformation" ("posBranchCode")',
        'CREATE INDEX "idx_posInformation_posHolderName" ON "posInformation" ("posHolderName")',
        'CREATE INDEX "idx_posInformation_region" ON "posInformation" ("region")',
        'CREATE INDEX "idx_posInformation_district" ON "posInformation" ("district")',
        'CREATE INDEX "idx_posInformation_qrFsrCode" ON "posInformation" ("qrFsrCode")',
        'CREATE INDEX "idx_posInformation_reportingDate" ON "posInformation" ("reportingDate")'
    ]
    
    for index_query in indexes:
        cursor.execute(index_query)
        print(f"✅ Created index: {index_query.split('CREATE INDEX ')[1].split(' ON')[0]}")
    
    # Create trigger for updatedAt
    trigger_query = """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW."updatedAt" = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    
    CREATE TRIGGER update_posInformation_updated_at 
        BEFORE UPDATE ON "posInformation" 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """
    
    cursor.execute(trigger_query)
    print("✅ Created updatedAt trigger")
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n🎉 posInformation table created successfully with camelCase naming!")
    print("📋 Table structure:")
    print("   - reportingDate (VARCHAR(20))")
    print("   - posBranchCode (INTEGER)")
    print("   - posNumber (VARCHAR(50)) - PRIMARY KEY")
    print("   - qrFsrCode (VARCHAR(50))")
    print("   - posHolderCategory (VARCHAR(50))")
    print("   - posHolderName (VARCHAR(100))")
    print("   - posHolderNin (VARCHAR(50)) - NULLABLE")
    print("   - posHolderTin (VARCHAR(50))")
    print("   - postalCode (VARCHAR(20)) - NULLABLE")
    print("   - region (VARCHAR(100))")
    print("   - district (VARCHAR(100))")
    print("   - ward (VARCHAR(100))")
    print("   - street (VARCHAR(100))")
    print("   - houseNumber (VARCHAR(50))")
    print("   - gpsCoordinates (TEXT) - NULLABLE")
    print("   - linkedAccount (VARCHAR(50))")
    print("   - issueDate (VARCHAR(20))")
    print("   - returnDate (VARCHAR(20)) - NULLABLE")
    print("   - createdAt (TIMESTAMP)")
    print("   - updatedAt (TIMESTAMP)")

if __name__ == "__main__":
    create_pos_information_table()