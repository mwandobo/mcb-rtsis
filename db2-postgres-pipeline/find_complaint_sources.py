#!/usr/bin/env python3
"""
Search for potential complaint/dispute data sources in PROFITS
"""

from db2_connection import DB2Connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db2_conn = DB2Connection()

try:
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. Search for transaction reversal/dispute tables
        logger.info("=" * 80)
        logger.info("1. SEARCHING FOR TRANSACTION REVERSAL/DISPUTE TABLES")
        logger.info("=" * 80)
        cursor.execute("""
            SELECT TABNAME, CARD, REMARKS
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA='PROFITS' 
            AND (
                TABNAME LIKE '%REVERS%'
                OR TABNAME LIKE '%DISPUTE%'
                OR TABNAME LIKE '%CHARGEBACK%'
                OR TABNAME LIKE '%REFUND%'
                OR TABNAME LIKE '%REJECT%'
                OR TABNAME LIKE '%ERROR%'
                OR TABNAME LIKE '%FAIL%'
            )
            AND CARD > 0
            ORDER BY CARD DESC
        """)
        
        tables = cursor.fetchall()
        if tables:
            logger.info(f"Found {len(tables)} tables with transaction disputes/reversals:")
            for idx, (table_name, card, remarks) in enumerate(tables[:20]):  # Top 20
                logger.info(f"  {table_name}: {card:,} rows")
                
                # Show structure of top table
                if idx == 0:
                    logger.info(f"\n  Structure of {table_name}:")
                    cursor.execute(f"""
                        SELECT COLNAME, TYPENAME, LENGTH 
                        FROM SYSCAT.COLUMNS 
                        WHERE TABSCHEMA='PROFITS' AND TABNAME='{table_name}'
                        ORDER BY COLNO
                    """)
                    cols = cursor.fetchall()
                    for col_name, col_type, col_len in cols[:15]:  # First 15 columns
                        logger.info(f"    {col_name}: {col_type}({col_len})")
        else:
            logger.info("No reversal/dispute tables found")
        
        # 2. Search for customer service/case tables
        logger.info("\n" + "=" * 80)
        logger.info("2. SEARCHING FOR CUSTOMER SERVICE/CASE MANAGEMENT TABLES")
        logger.info("=" * 80)
        cursor.execute("""
            SELECT TABNAME, CARD
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA='PROFITS' 
            AND (
                TABNAME LIKE '%CASE%'
                OR TABNAME LIKE '%SERVICE%'
                OR TABNAME LIKE '%REQUEST%'
                OR TABNAME LIKE '%TICKET%'
                OR TABNAME LIKE '%QUERY%'
            )
            AND CARD > 0
            AND TABNAME NOT LIKE '%HIST%'
            ORDER BY CARD DESC
        """)
        
        tables = cursor.fetchall()
        if tables:
            logger.info(f"Found {len(tables)} customer service tables:")
            for table_name, card in tables[:15]:
                logger.info(f"  {table_name}: {card:,} rows")
        else:
            logger.info("No customer service tables found")
        
        # 3. Search for fraud/suspicious activity tables
        logger.info("\n" + "=" * 80)
        logger.info("3. SEARCHING FOR FRAUD/SUSPICIOUS ACTIVITY TABLES")
        logger.info("=" * 80)
        cursor.execute("""
            SELECT TABNAME, CARD
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA='PROFITS' 
            AND (
                TABNAME LIKE '%FRAUD%'
                OR TABNAME LIKE '%SUSPECT%'
                OR TABNAME LIKE '%AML%'
                OR TABNAME LIKE '%SUSPICIOUS%'
                OR TABNAME LIKE '%ALERT%'
                OR TABNAME LIKE '%BLACK%'
            )
            AND CARD > 0
            ORDER BY CARD DESC
        """)
        
        tables = cursor.fetchall()
        if tables:
            logger.info(f"Found {len(tables)} fraud/suspicious activity tables:")
            for idx, (table_name, card) in enumerate(tables[:10]):
                logger.info(f"  {table_name}: {card:,} rows")
                
                # Show sample data from first table
                if idx == 0:
                    logger.info(f"\n  Sample from {table_name}:")
                    cursor.execute(f"SELECT * FROM {table_name} FETCH FIRST 2 ROWS ONLY")
                    rows = cursor.fetchall()
                    if rows:
                        cursor.execute(f"""
                            SELECT COLNAME FROM SYSCAT.COLUMNS 
                            WHERE TABSCHEMA='PROFITS' AND TABNAME='{table_name}'
                            ORDER BY COLNO
                        """)
                        col_names = [row[0] for row in cursor.fetchall()]
                        for i, row in enumerate(rows, 1):
                            logger.info(f"\n    Record {i}:")
                            for j, col_name in enumerate(col_names[:10]):  # First 10 columns
                                logger.info(f"      {col_name}: {row[j]}")
        else:
            logger.info("No fraud/suspicious activity tables found")
        
        # 4. Search for ATM/POS dispute tables
        logger.info("\n" + "=" * 80)
        logger.info("4. SEARCHING FOR ATM/POS/CARD DISPUTE TABLES")
        logger.info("=" * 80)
        cursor.execute("""
            SELECT TABNAME, CARD
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA='PROFITS' 
            AND (
                TABNAME LIKE '%ATM%'
                OR TABNAME LIKE '%POS%'
                OR TABNAME LIKE '%CARD%'
            )
            AND (
                TABNAME LIKE '%DISPUTE%'
                OR TABNAME LIKE '%FAIL%'
                OR TABNAME LIKE '%ERROR%'
                OR TABNAME LIKE '%REJECT%'
            )
            AND CARD > 0
            ORDER BY CARD DESC
        """)
        
        tables = cursor.fetchall()
        if tables:
            logger.info(f"Found {len(tables)} ATM/POS/Card dispute tables:")
            for table_name, card in tables:
                logger.info(f"  {table_name}: {card:,} rows")
        else:
            logger.info("No ATM/POS/Card dispute tables found")
        
        # 5. Check for any workflow/approval tables
        logger.info("\n" + "=" * 80)
        logger.info("5. SEARCHING FOR WORKFLOW/APPROVAL TABLES")
        logger.info("=" * 80)
        cursor.execute("""
            SELECT TABNAME, CARD
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA='PROFITS' 
            AND (
                TABNAME LIKE '%WORKFLOW%'
                OR TABNAME LIKE '%APPROVAL%'
                OR TABNAME LIKE '%AUTHORIS%'
                OR TABNAME LIKE '%PENDING%'
                OR TABNAME LIKE '%QUEUE%'
            )
            AND CARD > 0
            ORDER BY CARD DESC
        """)
        
        tables = cursor.fetchall()
        if tables:
            logger.info(f"Found {len(tables)} workflow/approval tables:")
            for table_name, card in tables[:10]:
                logger.info(f"  {table_name}: {card:,} rows")
        else:
            logger.info("No workflow/approval tables found")
        
        # 6. Search for log/audit tables that might track issues
        logger.info("\n" + "=" * 80)
        logger.info("6. SEARCHING FOR LOG/AUDIT TABLES")
        logger.info("=" * 80)
        cursor.execute("""
            SELECT TABNAME, CARD
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA='PROFITS' 
            AND (
                TABNAME LIKE '%LOG%'
                OR TABNAME LIKE '%AUDIT%'
                OR TABNAME LIKE '%TRACK%'
                OR TABNAME LIKE '%HISTORY%'
            )
            AND CARD > 10000  -- Only tables with significant data
            ORDER BY CARD DESC
        """)
        
        tables = cursor.fetchall()
        if tables:
            logger.info(f"Found {len(tables)} log/audit tables with >10k rows:")
            for table_name, card in tables[:10]:
                logger.info(f"  {table_name}: {card:,} rows")
        else:
            logger.info("No significant log/audit tables found")

except Exception as e:
    logger.error(f"Error: {e}")
    import traceback
    traceback.print_exc()
