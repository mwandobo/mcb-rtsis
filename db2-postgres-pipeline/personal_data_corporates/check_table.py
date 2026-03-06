#!/usr/bin/env python3
"""Check personalDataCorporates table status"""
import psycopg2, logging, sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def check_table():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    config = Config()
    
    try:
        conn = psycopg2.connect(host=config.database.pg_host, port=config.database.pg_port,
                               database=config.database.pg_database, user=config.database.pg_user,
                               password=config.database.pg_password)
        cursor = conn.cursor()
        
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'personalDataCorporates')")
        if not cursor.fetchone()[0]:
            logger.info("personalDataCorporates table does not exist")
            return
        
        logger.info("personalDataCorporates table exists")
        cursor.execute('SELECT COUNT(*) FROM "personalDataCorporates"')
        logger.info(f"Total records: {cursor.fetchone()[0]:,}")
        
        cursor.execute('SELECT "customerIdentificationNumber", "companyName", "region", created_at FROM "personalDataCorporates" LIMIT 5')
        logger.info("\nSample records:")
        for i, r in enumerate(cursor.fetchall(), 1):
            logger.info(f"  {i}. ID:{r[0]}, Company:{r[1]}, Region:{r[2]}")
        
        cursor.execute('SELECT "region", COUNT(*) FROM "personalDataCorporates" GROUP BY "region" ORDER BY COUNT(*) DESC LIMIT 10')
        logger.info("\nTop 10 Regions:")
        for region, count in cursor.fetchall():
            logger.info(f"  {region}: {count:,}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_table()
