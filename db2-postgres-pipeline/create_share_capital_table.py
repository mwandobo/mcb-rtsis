#!/usr/bin/env python3
"""
Create PostgreSQL table for share capital records
"""

import psycopg2
from config import Config

def create_share_capital_table():
    """Create the shareCapital table in PostgreSQL with camelCase fields"""
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
        
        # Drop table if exists
        cursor.execute('DROP TABLE IF EXISTS "shareCapital" CASCADE')
        
        # Create share capital table with all 14 fields using camelCase (quoted identifiers)
        create_table_query = """
        CREATE TABLE "shareCapital" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20) NOT NULL,
            "capitalCategory" VARCHAR(100),
            "capitalSubCategory" VARCHAR(100),
            "transactionDate" DATE,
            "transactionType" VARCHAR(50),
            "shareholderName" VARCHAR(255),
            "clientType" VARCHAR(100),
            "shareholderCountry" VARCHAR(100),
            "numberOfShares" INTEGER,
            "sharePriceBookValue" DECIMAL(15,4),
            "currency" VARCHAR(10),
            "orgAmount" DECIMAL(18,2),
            "tzsAmount" DECIMAL(18,2),
            "sectorSnaClassification" VARCHAR(100),
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance using camelCase (quoted identifiers)
        cursor.execute('CREATE INDEX "idxShareCapitalReportingDate" ON "shareCapital"("reportingDate")')
        cursor.execute('CREATE INDEX "idxShareCapitalTransactionDate" ON "shareCapital"("transactionDate")')
        cursor.execute('CREATE INDEX "idxShareCapitalCategory" ON "shareCapital"("capitalCategory")')
        cursor.execute('CREATE INDEX "idxShareCapitalSubCategory" ON "shareCapital"("capitalSubCategory")')
        cursor.execute('CREATE INDEX "idxShareCapitalTransactionType" ON "shareCapital"("transactionType")')
        cursor.execute('CREATE INDEX "idxShareCapitalCurrency" ON "shareCapital"("currency")')
        cursor.execute('CREATE INDEX "idxShareCapitalShareholderName" ON "shareCapital"("shareholderName")')
        cursor.execute('CREATE INDEX "idxShareCapitalCreatedAt" ON "shareCapital"("createdAt")')
        
        conn.commit()
        print("Share Capital table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'shareCapital' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\nTable structure ({len(columns)} columns):")
        for col in columns:
            nullable = 'NULL' if col[2] == 'YES' else 'NOT NULL'
            default = f" DEFAULT {col[3]}" if col[3] else ""
            print(f"  {col[0]}: {col[1]} ({nullable}){default}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error creating share capital table: {e}")
        raise

if __name__ == "__main__":
    create_share_capital_table()