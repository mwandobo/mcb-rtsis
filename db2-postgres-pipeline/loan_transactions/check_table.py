#!/usr/bin/env python3
"""
Check loanTransactions table status in PostgreSQL
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def check_loan_transactions_table():
    """Check the loanTransactions table status"""
    
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
                WHERE table_name = 'loanTransactions'
            )
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.info("loanTransactions table does not exist")
            return
        
        logger.info("loanTransactions table exists")
        
        # Get row count
        cursor.execute('SELECT COUNT(*) FROM "loanTransactions"')
        row_count = cursor.fetchone()[0]
        logger.info(f"Total records: {row_count:,}")
        
        # Get sample records
        cursor.execute('SELECT * FROM "loanTransactions" LIMIT 5')
        sample_records = cursor.fetchall()
        
        if sample_records:
            logger.info("\nSample records (first 5):")
            logger.info("-" * 80)
            for i, record in enumerate(sample_records, 1):
                logger.info(f"Record {i}:")
                logger.info(f"  Loan Number: {record[2]}")
                logger.info(f"  Transaction Date: {record[3]}")
                logger.info(f"  Transaction Type: {record[4]}")
                logger.info(f"  Currency: {record[6]}")
                logger.info(f"  Amount: {record[7]}")
                logger.info(f"  Created At: {record[10]}")
                logger.info("-" * 80)
        
        # Get statistics by transaction type
        cursor.execute("""
            SELECT "loanTransactionType", COUNT(*) as count
            FROM "loanTransactions"
            GROUP BY "loanTransactionType"
            ORDER BY count DESC
        """)
        
        transaction_type_stats = cursor.fetchall()
        
        if transaction_type_stats:
            logger.info("\nRecords by Transaction Type:")
            logger.info("-" * 80)
            for txn_type, count in transaction_type_stats:
                logger.info(f"{txn_type}: {count:,}")
            logger.info("-" * 80)
        
        # Get statistics by currency
        cursor.execute("""
            SELECT "currency", COUNT(*) as count
            FROM "loanTransactions"
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
        logger.error(f"Error checking loanTransactions table: {e}")
        raise

if __name__ == "__main__":
    check_loan_transactions_table()
