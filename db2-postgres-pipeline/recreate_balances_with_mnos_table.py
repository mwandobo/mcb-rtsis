#!/usr/bin/env python3
"""
Recreate balancesWithMnos table without id column
"""

import psycopg2
from config import Config

def recreate_balances_mnos_table():
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
        print("Dropping existing balancesWithMnos table...")
        cursor.execute('DROP TABLE IF EXISTS "balancesWithMnos" CASCADE;')
        conn.commit()
        print("✓ Table dropped")
        
        # Create new table without id
        print("Creating new balancesWithMnos table...")
        create_table_sql = """
        CREATE TABLE "balancesWithMnos" (
            "reportingDate" VARCHAR(20),
            "floatBalanceDate" VARCHAR(20),
            "mnoCode" VARCHAR(50),
            "tillNumber" VARCHAR(50),
            "currency" VARCHAR(10),
            "allowanceProbableLoss" DECIMAL(18,2),
            "botProvision" DECIMAL(18,2),
            "orgFloatAmount" DECIMAL(18,2),
            "usdFloatAmount" DECIMAL(18,2),
            "tzsFloatAmount" DECIMAL(18,2)
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        print("✓ Table created successfully")
        
        # Verify table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'balancesWithMnos'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"\nTable structure ({len(columns)} columns):")
        for col_name, col_type in columns:
            print(f"  - {col_name}: {col_type}")
        
        cursor.close()
        conn.close()
        
        print("\n✓ balancesWithMnos table recreated successfully!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise

if __name__ == "__main__":
    recreate_balances_mnos_table()
