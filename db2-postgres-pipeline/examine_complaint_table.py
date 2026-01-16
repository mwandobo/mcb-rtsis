#!/usr/bin/env python3
"""
Examine BOT_91_DISPUTE table structure for complaint statistics
"""

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db2_conn = DB2Connection()

try:
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get table structure
        logger.info("BOT_91_DISPUTE table structure:")
        cursor.execute("""
            SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS, REMARKS
            FROM SYSCAT.COLUMNS 
            WHERE TABSCHEMA='PROFITS' AND TABNAME='BOT_91_DISPUTE'
            ORDER BY COLNO
        """)
        
        columns = cursor.fetchall()
        for col_name, col_type, col_len, scale, nulls, remarks in columns:
            nullable = "NULL" if nulls == 'Y' else "NOT NULL"
            scale_info = f",{scale}" if scale else ""
            logger.info(f"  {col_name}: {col_type}({col_len}{scale_info}) {nullable}")
            if remarks:
                logger.info(f"    Remarks: {remarks}")
        
        # Get sample data
        logger.info("\nSample data from BOT_91_DISPUTE:")
        cursor.execute("SELECT * FROM BOT_91_DISPUTE FETCH FIRST 5 ROWS ONLY")
        rows = cursor.fetchall()
        
        if rows:
            for i, row in enumerate(rows, 1):
                logger.info(f"\nRecord {i}:")
                for j, col in enumerate(columns):
                    logger.info(f"  {col[0]}: {row[j]}")
        else:
            logger.info("No data found in BOT_91_DISPUTE table")
            
        # Check related tables
        logger.info("\nChecking related BOT tables for complaints...")
        cursor.execute("""
            SELECT TABNAME, CARD 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA='PROFITS' 
            AND TABNAME LIKE 'BOT_%'
            AND TABNAME NOT LIKE '%HIST%'
            ORDER BY TABNAME
        """)
        
        bot_tables = cursor.fetchall()
        logger.info(f"\nFound {len(bot_tables)} BOT tables:")
        for table_name, card in bot_tables:
            logger.info(f"  - {table_name} (Rows: {card})")

except Exception as e:
    logger.error(f"Error: {e}")
    import traceback
    traceback.print_exc()
