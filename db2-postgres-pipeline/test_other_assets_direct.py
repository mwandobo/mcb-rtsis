#!/usr/bin/env python3
"""
Direct test for Other Assets - fetch from DB2 and insert to PostgreSQL
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db2_connection import DB2Connection
from processors.other_assets_processor import OtherAssetsProcessor
import psycopg2
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_direct_other_assets():
    """Direct test to fetch and insert other assets data"""
    
    # Simplified query that works
    simple_query = """
    SELECT
        CURRENT_TIMESTAMP AS reportingDate,
        CASE
            WHEN gl.EXTERNAL_GLACCOUNT = '100017000' THEN 'Gold'
            WHEN gl.EXTERNAL_GLACCOUNT = '144000032' THEN 'StampAccount'
            WHEN gl.EXTERNAL_GLACCOUNT IN ('144000015','144000047','144000048','144000050') THEN 'SundryDebtors'
            ELSE 'MiscellaneousAssets'
        END AS assetType,
        gte.TRN_DATE AS transactionDate,
        gte.AVAILABILITY_DATE AS maturityDate,
        COALESCE(WC.FIRST_NAME, 'Unknown Debtor') AS debtorName,
        'Tanzania' AS debtorCountry,
        gte.CURRENCY_SHORT_DES AS currency,
        gte.DC_AMOUNT AS orgAmount,
        CASE WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT ELSE NULL END AS usdAmount,
        CASE WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2730.50 ELSE gte.DC_AMOUNT END AS tzsAmount,
        'Other Non-Financial Corporations' AS sectorSnaClassification,
        0 AS pastDueDays,
        1 AS assetClassificationCategory,
        0 AS allowanceProbableLoss,
        0 AS botProvision
    FROM GLI_TRX_EXTRACT AS gte
    LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
    LEFT JOIN W_DIM_CUSTOMER AS WC ON WC.CUST_ID = gte.CUST_ID
    WHERE gl.EXTERNAL_GLACCOUNT IN ('100017000','144000032','144000015','144000047','144000048','144000050')
    FETCH FIRST 10 ROWS ONLY
    """
    
    config = Config()
    
    try:
        logger.info("Testing direct other assets insertion...")
        
        # Connect to DB2
        db2_conn = DB2Connection()
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        
        # Initialize processor
        processor = OtherAssetsProcessor()
        
        records_processed = 0
        records_inserted = 0
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(simple_query)
            
            while True:
                rows = cursor.fetchmany(10)
                if not rows:
                    break
                    
                for row in rows:
                    try:
                        # Process the record
                        record = processor.process_record(row, 'other_assets')
                        
                        # Validate the record
                        if processor.validate_record(record):
                            # Insert to PostgreSQL
                            processor.insert_to_postgres(record, pg_cursor)
                            records_inserted += 1
                            
                            logger.info(f"Inserted: {record.asset_type}, {record.debtor_name}, {record.org_amount:,.2f} {record.currency}")
                        else:
                            logger.warning(f"Invalid record skipped: {record}")
                            
                        records_processed += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing record: {e}")
                        logger.error(f"Raw data: {row}")
                        continue
        
        # Commit PostgreSQL transaction
        pg_conn.commit()
        
        logger.info(f"Direct test completed successfully!")
        logger.info(f"Records processed: {records_processed}")
        logger.info(f"Records inserted: {records_inserted}")
        
        # Verify data in PostgreSQL
        pg_cursor.execute('SELECT COUNT(*) FROM other_assets')
        total_count = pg_cursor.fetchone()[0]
        logger.info(f"Total records in other_assets table: {total_count}")
        
        pg_conn.close()
        
    except Exception as e:
        logger.error(f"Direct test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_direct_other_assets()