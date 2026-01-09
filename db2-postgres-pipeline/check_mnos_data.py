#!/usr/bin/env python3
"""
Check MNOs data to understand why only 2 records
"""

import psycopg2
import logging
from config import Config

def check_mnos_data():
    """Check the MNOs data in detail"""
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
        
        logger.info("🔍 CHECKING BALANCE WITH MNOS DATA")
        logger.info("=" * 60)
        
        # Check all records in detail
        cursor.execute("""
            SELECT "id", "tillNumber", "mnoCode", "reportingDate", "orgFloatAmount", "currency"
            FROM "balanceWithMnos" 
            ORDER BY "reportingDate" DESC;
        """)
        
        records = cursor.fetchall()
        logger.info(f"📊 Total records in balanceWithMnos: {len(records)}")
        
        for record in records:
            record_id, till_number, mno_code, reporting_date, org_float_amount, currency = record
            logger.info(f"  ID: {record_id} | Till: {till_number} | MNO: {mno_code}")
            logger.info(f"    Date: {reporting_date} | Amount: {org_float_amount} {currency}")
            logger.info("")
        
        # Check the unique constraint
        cursor.execute("""
            SELECT constraint_name, constraint_type 
            FROM information_schema.table_constraints 
            WHERE table_name = 'balanceWithMnos' AND constraint_type = 'UNIQUE';
        """)
        
        constraints = cursor.fetchall()
        logger.info("🔒 UNIQUE CONSTRAINTS:")
        for constraint_name, constraint_type in constraints:
            logger.info(f"  {constraint_name}: {constraint_type}")
        
        # Check what the unique constraint covers
        cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc 
                ON kcu.constraint_name = tc.constraint_name
            WHERE tc.table_name = 'balanceWithMnos' 
                AND tc.constraint_type = 'UNIQUE'
            ORDER BY kcu.ordinal_position;
        """)
        
        unique_columns = cursor.fetchall()
        logger.info("📋 UNIQUE CONSTRAINT COLUMNS:")
        for column in unique_columns:
            logger.info(f"  - {column[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to check MNOs data: {e}")
        raise

if __name__ == "__main__":
    check_mnos_data()