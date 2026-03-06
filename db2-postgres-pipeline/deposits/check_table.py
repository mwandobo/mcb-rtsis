#!/usr/bin/env python3
"""
Check deposits table contents
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_deposits_table():
    """Check deposits table"""
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
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'deposits'
            )
        """)
        exists = cursor.fetchone()[0]
        
        if not exists:
            logger.error("deposits table does not exist")
            return
        
        logger.info("deposits table exists")
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM "deposits"')
        total = cursor.fetchone()[0]
        logger.info(f"Total records: {total:,}")
        
        # Get sample records
        cursor.execute("""
            SELECT "clientIdentificationNumber", "accountNumber", "accountName", "region", "currency"
            FROM "deposits"
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        logger.info("\nSample records:")
        for i, sample in enumerate(samples, 1):
            logger.info(f"  {i}. Client:{sample[0]}, Account:{sample[1]}, Name:{sample[2]}, Region:{sample[3]}, Currency:{sample[4]}")
        
        # Get region distribution
        cursor.execute("""
            SELECT "region", COUNT(*) as count
            FROM "deposits"
            GROUP BY "region"
            ORDER BY count DESC
            LIMIT 10
        """)
        regions = cursor.fetchall()
        
        logger.info("\nTop 10 Regions:")
        for region, count in regions:
            logger.info(f"  {region}: {count:,}")
        
        # Get currency distribution
        cursor.execute("""
            SELECT "currency", COUNT(*) as count
            FROM "deposits"
            GROUP BY "currency"
            ORDER BY count DESC
        """)
        currencies = cursor.fetchall()
        
        logger.info("\nCurrency Distribution:")
        for currency, count in currencies:
            logger.info(f"  {currency}: {count:,}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error checking deposits table: {e}")
        raise


if __name__ == "__main__":
    check_deposits_table()
