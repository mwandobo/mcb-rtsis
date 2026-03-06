#!/usr/bin/env python3
"""Drop personalDataCorporates table"""
import psycopg2, logging, sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def drop_table():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    config = Config()
    
    try:
        conn = psycopg2.connect(host=config.database.pg_host, port=config.database.pg_port,
                               database=config.database.pg_database, user=config.database.pg_user,
                               password=config.database.pg_password)
        cursor = conn.cursor()
        logger.info("Dropping personalDataCorporates table...")
        cursor.execute('DROP TABLE IF EXISTS "personalDataCorporates" CASCADE')
        conn.commit()
        logger.info("Table dropped successfully!")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    drop_table()
