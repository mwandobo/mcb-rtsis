#!/usr/bin/env python3
"""
Check personalData table status in PostgreSQL
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def check_personal_data_table():
    """Check the personalDataInformation table status"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'personalDataInformation'
            )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.info("personalDataInformation table does not exist")
            return
        
        logger.info("personalDataInformation table exists")
        
        # Get row count
        cursor.execute('SELECT COUNT(*) FROM "personalDataInformation"')
        row_count = cursor.fetchone()[0]
        logger.info(f"Total records: {row_count:,}")
        
        # Get sample records
        cursor.execute('SELECT "customerIdentificationNumber", "fullNames", "gender", "region", "mobileNumber", created_at FROM "personalDataInformation" LIMIT 5')
        sample_records = cursor.fetchall()
        
        if sample_records:
            logger.info("\nSample records (first 5):")
            logger.info("-" * 80)
            for i, record in enumerate(sample_records, 1):
                logger.info(f"Record {i}:")
                logger.info(f"  Customer ID: {record[0]}")
                logger.info(f"  Full Names: {record[1]}")
                logger.info(f"  Gender: {record[2]}")
                logger.info(f"  Region: {record[3]}")
                logger.info(f"  Mobile: {record[4]}")
                logger.info(f"  Created At: {record[5]}")
                logger.info("-" * 80)
        
        # Get statistics by gender
        cursor.execute("""
            SELECT "gender", COUNT(*) as count
            FROM "personalDataInformation"
            GROUP BY "gender"
            ORDER BY count DESC
        """)
        
        gender_stats = cursor.fetchall()
        
        if gender_stats:
            logger.info("\nRecords by Gender:")
            logger.info("-" * 80)
            for gender, count in gender_stats:
                logger.info(f"{gender}: {count:,}")
            logger.info("-" * 80)
        
        # Get statistics by region
        cursor.execute("""
            SELECT "region", COUNT(*) as count
            FROM "personalDataInformation"
            GROUP BY "region"
            ORDER BY count DESC
            LIMIT 10
        """)
        
        region_stats = cursor.fetchall()
        
        if region_stats:
            logger.info("\nRecords by Region (Top 10):")
            logger.info("-" * 80)
            for region, count in region_stats:
                logger.info(f"{region}: {count:,}")
            logger.info("-" * 80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error checking personalDataInformation table: {e}")
        raise

if __name__ == "__main__":
    check_personal_data_table()
