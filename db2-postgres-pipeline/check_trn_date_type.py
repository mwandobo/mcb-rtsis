#!/usr/bin/env python3
"""
Check TRN_DATE field type and find proper timestamp field
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_trn_date_structure():
    """Check the structure of TRN_DATE and find timestamp fields"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check column information for GLI_TRX_EXTRACT table
            logger.info("🔍 Checking GLI_TRX_EXTRACT table structure...")
            
            column_query = """
            SELECT 
                COLNAME,
                TYPENAME,
                LENGTH,
                SCALE,
                NULLS,
                REMARKS
            FROM SYSCAT.COLUMNS 
            WHERE TABSCHEMA = 'PROFITS' 
            AND TABNAME = 'GLI_TRX_EXTRACT'
            AND (COLNAME LIKE '%DATE%' OR COLNAME LIKE '%TIME%' OR COLNAME LIKE '%STAMP%')
            ORDER BY COLNAME
            """
            
            cursor.execute(column_query)
            columns = cursor.fetchall()
            
            logger.info("📋 Date/Time/Timestamp columns in GLI_TRX_EXTRACT:")
            for col in columns:
                logger.info(f"  - {col[0]} | Type: {col[1]} | Length: {col[2]} | Nullable: {col[4]}")
            
            # Check sample data to see actual values
            logger.info("\n📊 Sample data from GLI_TRX_EXTRACT:")
            
            sample_query = """
            SELECT 
                TRN_DATE,
                TYPEOF(TRN_DATE) as TRN_DATE_TYPE,
                AVAILABILITY_DATE,
                TYPEOF(AVAILABILITY_DATE) as AVAIL_DATE_TYPE
            FROM GLI_TRX_EXTRACT 
            WHERE TRN_DATE IS NOT NULL
            FETCH FIRST 5 ROWS ONLY
            """
            
            cursor.execute(sample_query)
            samples = cursor.fetchall()
            
            for i, row in enumerate(samples, 1):
                logger.info(f"  {i}. TRN_DATE: {row[0]} (Type: {row[1]}) | AVAILABILITY_DATE: {row[2]} (Type: {row[3]})")
            
            # Check if there are any timestamp columns
            logger.info("\n🔍 Looking for timestamp columns...")
            
            timestamp_query = """
            SELECT 
                COLNAME,
                TYPENAME
            FROM SYSCAT.COLUMNS 
            WHERE TABSCHEMA = 'PROFITS' 
            AND TABNAME = 'GLI_TRX_EXTRACT'
            AND TYPENAME IN ('TIMESTAMP', 'TIMESTMP')
            ORDER BY COLNAME
            """
            
            cursor.execute(timestamp_query)
            timestamp_cols = cursor.fetchall()
            
            if timestamp_cols:
                logger.info("📅 Found timestamp columns:")
                for col in timestamp_cols:
                    logger.info(f"  - {col[0]} | Type: {col[1]}")
                    
                # Get sample data from timestamp columns
                ts_col_name = timestamp_cols[0][0]
                sample_ts_query = f"""
                SELECT {ts_col_name}
                FROM GLI_TRX_EXTRACT 
                WHERE {ts_col_name} IS NOT NULL
                ORDER BY {ts_col_name} DESC
                FETCH FIRST 5 ROWS ONLY
                """
                
                cursor.execute(sample_ts_query)
                ts_samples = cursor.fetchall()
                
                logger.info(f"\n📊 Sample {ts_col_name} values:")
                for i, row in enumerate(ts_samples, 1):
                    logger.info(f"  {i}. {row[0]}")
                    
            else:
                logger.warning("⚠️ No timestamp columns found in GLI_TRX_EXTRACT")
                
                # Check what we can use for tracking
                logger.info("🔍 Checking alternative tracking options...")
                
                alt_query = """
                SELECT 
                    TRN_DATE,
                    AVAILABILITY_DATE,
                    CURRENT_TIMESTAMP as CURRENT_TS
                FROM GLI_TRX_EXTRACT 
                ORDER BY TRN_DATE DESC
                FETCH FIRST 3 ROWS ONLY
                """
                
                cursor.execute(alt_query)
                alt_samples = cursor.fetchall()
                
                logger.info("📊 Alternative tracking fields:")
                for i, row in enumerate(alt_samples, 1):
                    logger.info(f"  {i}. TRN_DATE: {row[0]} | AVAILABILITY_DATE: {row[1]} | Current: {row[2]}")
            
    except Exception as e:
        logger.error(f"❌ Failed to check table structure: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_trn_date_structure()