#!/usr/bin/env python3
"""
Check loanInformation table status in PostgreSQL
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def check_loans_table():
    """Check the loanInformation table status"""
    
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
                WHERE table_name = 'loanInformation'
            )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.info("loanInformation table does not exist")
            return
        
        logger.info("loanInformation table exists")
        
        # Get row count
        cursor.execute('SELECT COUNT(*) FROM "loanInformation"')
        row_count = cursor.fetchone()[0]
        logger.info(f"Total records: {row_count:,}")
        
        # Get sample records
        cursor.execute('SELECT * FROM "loanInformation" LIMIT 5')
        sample_records = cursor.fetchall()
        
        if sample_records:
            logger.info("\nSample records (first 5):")
            logger.info("-" * 80)
            for i, record in enumerate(sample_records, 1):
                logger.info(f"Record {i}:")
                logger.info(f"  Loan Number: {record[18]}")
                logger.info(f"  Customer ID: {record[2]}")
                logger.info(f"  Client Name: {record[4]}")
                logger.info(f"  Loan Type: {record[19]}")
                logger.info(f"  Currency: {record[34]}")
                logger.info(f"  Created At: {record[56]}")
                logger.info("-" * 80)
        
        # Get statistics by loan type
        cursor.execute("""
            SELECT "loanType", COUNT(*) as count
            FROM "loanInformation"
            GROUP BY "loanType"
            ORDER BY count DESC
            LIMIT 10
        """)
        
        loan_type_stats = cursor.fetchall()
        
        if loan_type_stats:
            logger.info("\nRecords by Loan Type (Top 10):")
            logger.info("-" * 80)
            for loan_type, count in loan_type_stats:
                logger.info(f"{loan_type}: {count:,}")
            logger.info("-" * 80)
        
        # Get statistics by currency
        cursor.execute("""
            SELECT "currency", COUNT(*) as count
            FROM "loanInformation"
            GROUP BY "currency"
            ORDER BY count DESC
        """)
        
        currency_stats = cursor.fetchall()
        
        if currency_stats:
            logger.info("\nRecords by Currency:")
            logger.info("-" * 80)
            for currency, count in currency_stats:
                logger.info(f"{currency}: {count:,}")
            logger.info("-" * 80)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error checking loanInformation table: {e}")
        raise

if __name__ == "__main__":
    check_loans_table()
