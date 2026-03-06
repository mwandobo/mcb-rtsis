#!/usr/bin/env python3
"""
Create cards table in PostgreSQL
"""

import psycopg2
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


def create_cards_table():
    """Create cards table in PostgreSQL"""
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
        
        # Drop table if exists (optional - comment out if you want to preserve data)
        # cursor.execute('DROP TABLE IF EXISTS "cardInformation" CASCADE')
        # print("Dropped existing cardInformation table")
        
        # Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS "cardInformation" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "bankCode" VARCHAR(50),
            "cardNumber" VARCHAR(100) NOT NULL,
            "binNumber" VARCHAR(50),
            "customerIdentificationNumber" VARCHAR(100),
            "cardType" VARCHAR(50),
            "cardTypeSubCategory" VARCHAR(100),
            "cardIssueDate" VARCHAR(50),
            "cardIssuer" VARCHAR(200),
            "cardIssuerCategory" VARCHAR(100),
            "cardIssuerCountry" VARCHAR(100),
            "cardHolderName" VARCHAR(200),
            "cardStatus" VARCHAR(50),
            "cardScheme" VARCHAR(100),
            "acquiringPartner" VARCHAR(200),
            "cardExpireDate" VARCHAR(50),
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        print("Created cardInformation table")
        
        # Create unique index on cardNumber for duplicate prevention
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_cardinformation_cardnumber_unique
            ON "cardInformation" ("cardNumber")
        """)
        print("Created unique index on cardNumber")
        
        # Create additional indexes for common queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cardinformation_customer_id
            ON "cardInformation" ("customerIdentificationNumber")
        """)
        print("Created index on customerIdentificationNumber")
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cardinformation_status
            ON "cardInformation" ("cardStatus")
        """)
        print("Created index on cardStatus")
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cardinformation_type
            ON "cardInformation" ("cardType")
        """)
        print("Created index on cardType")
        
        conn.commit()
        print("cardInformation table created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'cardInformation'
            ORDER BY ordinal_position
        """)
        
        print("\nTable structure:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}" + (f"({row[2]})" if row[2] else ""))
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating cardInformation table: {e}")
        raise


if __name__ == "__main__":
    create_cards_table()
