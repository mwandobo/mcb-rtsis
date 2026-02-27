#!/usr/bin/env python3
"""
Recreate cardInformation table without id column
"""

import psycopg2
from config import Config

def recreate_card_table():
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
        
        # Drop existing table
        print("Dropping existing cardInformation table...")
        cursor.execute('DROP TABLE IF EXISTS "cardInformation" CASCADE;')
        conn.commit()
        print("✓ Table dropped")
        
        # Create new table without id
        print("Creating new cardInformation table...")
        create_table_sql = """
        CREATE TABLE "cardInformation" (
            "reportingDate" VARCHAR(20),
            "bankCode" VARCHAR(50),
            "cardNumber" VARCHAR(50),
            "binNumber" VARCHAR(50),
            "customerIdentificationNumber" VARCHAR(50),
            "cardType" VARCHAR(50),
            "cardTypeSubCategory" VARCHAR(50),
            "cardIssueDate" VARCHAR(20),
            "cardIssuer" VARCHAR(255),
            "cardIssuerCategory" VARCHAR(50),
            "cardIssuerCountry" VARCHAR(100),
            "cardHolderName" VARCHAR(255),
            "cardStatus" VARCHAR(50),
            "cardScheme" VARCHAR(50),
            "acquiringPartner" VARCHAR(255),
            "cardExpireDate" VARCHAR(20)
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        print("✓ Table created successfully")
        
        # Verify table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'cardInformation'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"\nTable structure ({len(columns)} columns):")
        for col_name, col_type in columns:
            print(f"  - {col_name}: {col_type}")
        
        cursor.close()
        conn.close()
        
        print("\n✓ cardInformation table recreated successfully!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise

if __name__ == "__main__":
    recreate_card_table()
