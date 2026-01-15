#!/usr/bin/env python3
"""
Simple Balance with MNOs Pipeline
"""

import psycopg2
import logging
from config import Config
from db2_connection import DB2Connection
from processors.mnos_processor import MnosProcessor

def run_simple_mnos():
    """Run simple balance with MNOs pipeline"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("📱 SIMPLE BALANCE WITH MNOS PIPELINE")
    logger.info("=" * 60)
    
    try:
        # Initialize connections
        db2_conn = DB2Connection()
        processor = MnosProcessor()
        
        # Get table configuration
        table_config = config.tables['balanceWithMnos']
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        logger.info("✅ Connected to PostgreSQL")
        
        # Execute query and process records directly
        logger.info("🔍 Fetching data from DB2...")
        
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(table_config.query)
            
            logger.info("📊 Processing records...")
            
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                
                try:
                    # Process the record
                    record = processor.process_record(row, table_config.name)
                    
                    # Validate record
                    if processor.validate_record(record):
                        # Insert directly to PostgreSQL
                        processor.insert_to_postgres(record, pg_cursor)
                        processed_count += 1
                        
                        if processed_count % 50 == 0:
                            logger.info(f"✅ Processed {processed_count} MNO records")
                            pg_conn.commit()
                    else:
                        skipped_count += 1
                        logger.warning(f"⚠️ Invalid record skipped: {record.mno_code if hasattr(record, 'mno_code') else 'Unknown'}")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Error processing record: {e}")
                    continue
        
        # Final commit
        pg_conn.commit()
        
        # Check final count
        pg_cursor.execute('SELECT COUNT(*) FROM "balanceWithMnos"')
        total_count = pg_cursor.fetchone()[0]
        
        logger.info("=" * 60)
        logger.info(f"📊 PIPELINE SUMMARY:")
        logger.info(f"   - Successfully processed: {processed_count}")
        logger.info(f"   - Skipped (validation): {skipped_count}")
        logger.info(f"   - Errors: {error_count}")
        logger.info(f"   - Total in PostgreSQL: {total_count}")
        
        # Show sample records
        pg_cursor.execute("""
            SELECT "mnoCode", "tillNumber", "currency", "orgFloatAmount", "tzsFloatAmount"
            FROM "balanceWithMnos" 
            ORDER BY "reportingDate" DESC 
            LIMIT 5
        """)
        
        sample_records = pg_cursor.fetchall()
        logger.info("📋 Sample records:")
        for record in sample_records:
            mno_code, till_number, currency, org_amount, tzs_amount = record
            logger.info(f"  - {mno_code:<15} Till: {till_number} {currency} Org: {org_amount} TZS: {tzs_amount}")
        
        # Show MNO distribution
        pg_cursor.execute("""
            SELECT 
                "mnoCode", 
                COUNT(*) as count,
                SUM("tzsFloatAmount") as total_tzs
            FROM "balanceWithMnos" 
            GROUP BY "mnoCode" 
            ORDER BY COUNT(*) DESC
        """)
        
        mno_stats = pg_cursor.fetchall()
        logger.info("📊 MNO Distribution:")
        for mno_code, count, total_tzs in mno_stats:
            logger.info(f"  - {mno_code}: {count} records, Total TZS: {total_tzs:,.2f}")
        
        # Show currency distribution
        pg_cursor.execute("""
            SELECT 
                "currency", 
                COUNT(*) as count
            FROM "balanceWithMnos" 
            GROUP BY "currency" 
            ORDER BY COUNT(*) DESC
        """)
        
        currencies = pg_cursor.fetchall()
        logger.info("📊 Currency Distribution:")
        for currency, count in currencies:
            logger.info(f"  - {currency}: {count} records")
        
        # Close connections
        pg_cursor.close()
        pg_conn.close()
        
        logger.info("✅ Balance with MNOs pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error in MNOs pipeline: {e}")
        raise

if __name__ == "__main__":
    run_simple_mnos()