#!/usr/bin/env python3
"""
Find complaint-related tables in PROFITS database
"""

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db2_conn = DB2Connection()

try:
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Search for complaint-related tables
        logger.info("Searching for complaint-related tables...")
        query = """
        SELECT TABNAME, CARD, REMARKS 
        FROM SYSCAT.TABLES 
        WHERE TABSCHEMA='PROFITS' 
        AND (
            TABNAME LIKE '%COMPLAINT%' 
            OR TABNAME LIKE '%COMPLAIN%' 
            OR TABNAME LIKE '%DISPUTE%' 
            OR TABNAME LIKE '%ISSUE%'
            OR TABNAME LIKE '%INCIDENT%'
            OR TABNAME LIKE '%CLAIM%'
            OR TABNAME LIKE '%GRIEVANCE%'
        )
        ORDER BY TABNAME
        """
        
        cursor.execute(query)
        tables = cursor.fetchall()
        
        if tables:
            logger.info(f"Found {len(tables)} complaint-related tables:")
            for table_name, card, remarks in tables:
                logger.info(f"  - {table_name} (Rows: {card})")
        else:
            logger.info("No complaint-related tables found")
            
        # Also search for customer service or feedback tables
        logger.info("\nSearching for customer service/feedback tables...")
        query2 = """
        SELECT TABNAME, CARD 
        FROM SYSCAT.TABLES 
        WHERE TABSCHEMA='PROFITS' 
        AND (
            TABNAME LIKE '%CUSTOMER%SERVICE%'
            OR TABNAME LIKE '%FEEDBACK%'
            OR TABNAME LIKE '%TICKET%'
            OR TABNAME LIKE '%CASE%'
        )
        ORDER BY TABNAME
        """
        
        cursor.execute(query2)
        tables2 = cursor.fetchall()
        
        if tables2:
            logger.info(f"Found {len(tables2)} customer service tables:")
            for table_name, card in tables2:
                logger.info(f"  - {table_name} (Rows: {card})")
        else:
            logger.info("No customer service tables found")
            
        # Search all tables with columns that might contain complaint data
        logger.info("\nSearching for tables with complaint-related columns...")
        query3 = """
        SELECT DISTINCT TABNAME, COUNT(*) as col_count
        FROM SYSCAT.COLUMNS 
        WHERE TABSCHEMA='PROFITS' 
        AND (
            COLNAME LIKE '%COMPLAINT%'
            OR COLNAME LIKE '%COMPLAIN%'
            OR COLNAME LIKE '%DISPUTE%'
            OR COLNAME LIKE '%ISSUE%'
            OR COLNAME LIKE '%INCIDENT%'
        )
        GROUP BY TABNAME
        ORDER BY col_count DESC, TABNAME
        """
        
        cursor.execute(query3)
        tables3 = cursor.fetchall()
        
        if tables3:
            logger.info(f"Found {len(tables3)} tables with complaint-related columns:")
            for table_name, col_count in tables3:
                logger.info(f"  - {table_name} ({col_count} complaint columns)")
                
                # Show the columns
                cursor.execute(f"""
                    SELECT COLNAME, TYPENAME, LENGTH 
                    FROM SYSCAT.COLUMNS 
                    WHERE TABSCHEMA='PROFITS' AND TABNAME='{table_name}'
                    AND (
                        COLNAME LIKE '%COMPLAINT%'
                        OR COLNAME LIKE '%COMPLAIN%'
                        OR COLNAME LIKE '%DISPUTE%'
                        OR COLNAME LIKE '%ISSUE%'
                        OR COLNAME LIKE '%INCIDENT%'
                    )
                    ORDER BY COLNAME
                """)
                cols = cursor.fetchall()
                for col_name, col_type, col_len in cols:
                    logger.info(f"      {col_name}: {col_type}({col_len})")
        else:
            logger.info("No tables with complaint-related columns found")

except Exception as e:
    logger.error(f"Error: {e}")
    import traceback
    traceback.print_exc()
