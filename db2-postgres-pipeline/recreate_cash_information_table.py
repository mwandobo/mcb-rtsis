#!/usr/bin/env python3
"""
Recreate cashInformation table without ID column
"""

import psycopg2
import logging
from config import Config

def recreate_cash_information_table():
    """Drop and recreate cashInformation table without ID"""
    
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
        
        logger.info("Dropping existing cashInformation table...")
        cursor.execute('DROP TABLE IF EXISTS "cashInformation" CASCADE')
        conn.commit()
        logger.info("✓ Table dropped")
        
        logger.info("Creating new cashInformation table without ID...")
        create_table_sql = """
        CREATE TABLE "cashInformation" (
            "reportingDate" VARCHAR(50),
            "branchCode" VARCHAR(50),
            "cashCategory" VARCHAR(100),
            "cashSubCategory" VARCHAR(100),
            "cashSubmissionTime" VARCHAR(50),
            currency VARCHAR(10),
            "cashDenomination" VARCHAR(50),
            "quantityOfCoinsNotes" INTEGER,
            "orgAmount" DECIMAL(18, 2),
            "usdAmount" DECIMAL(18, 2),
            "tzsAmount" DECIMAL(18, 2),
            "transactionDate" VARCHAR(50),
            "maturityDate" VARCHAR(50),
            "allowanceProbableLoss" DECIMAL(18, 2),
            "botProvision" DECIMAL(18, 2)
        )
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("✓ Table created successfully")
        
        # Create indexes for better query performance
        logger.info("Creating indexes...")
        
        cursor.execute('CREATE INDEX idx_cash_branch ON "cashInformation"("branchCode")')
        cursor.execute('CREATE INDEX idx_cash_category ON "cashInformation"("cashCategory")')
        cursor.execute('CREATE INDEX idx_cash_currency ON "cashInformation"(currency)')
        cursor.execute('CREATE INDEX idx_cash_transaction_date ON "cashInformation"("transactionDate")')
        
        conn.commit()
        logger.info("✓ Indexes created")
        
        # Show table structure
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_name = 'cashInformation'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("\nTable structure:")
        logger.info("-" * 80)
        for col in columns:
            col_name, data_type, char_len, num_prec, num_scale = col
            if char_len:
                logger.info(f"  {col_name:30} {data_type}({char_len})")
            elif num_prec:
                logger.info(f"  {col_name:30} {data_type}({num_prec},{num_scale})")
            else:
                logger.info(f"  {col_name:30} {data_type}")
        logger.info("-" * 80)
        
        cursor.close()
        conn.close()
        
        logger.info("\n✓ cashInformation table recreated successfully (without ID column)")
        
    except Exception as e:
        logger.error(f"✗ Error recreating table: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    print("=" * 60)
    print("Recreate cashInformation Table (without ID)")
    print("=" * 60)
    print("\nWARNING: This will DROP the existing table and all its data!")
    
    response = input("\nDo you want to continue? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        recreate_cash_information_table()
    else:
        print("Operation cancelled.")
