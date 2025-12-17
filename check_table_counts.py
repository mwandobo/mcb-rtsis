#!/usr/bin/env python3
"""
Check table counts for securities-related tables
"""

import logging
import sys
from db2_connection import DB2Connection
from config import Config

def check_table_counts():
    """Check record counts for potential securities tables"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 Checking Table Record Counts")
    logger.info("=" * 60)
    
    try:
        # Connect to DB2
        db2_conn = DB2Connection()
        
        # Tables to check
        tables_to_check = [
            'TRBOND',
            'RSKCO_SECURITIES', 
            'GLI_TRX_EXTRACT',
            'SECURITIES',
            'SECURITIES_GL',
            'CUST_SECURITIES',
            'COLLATERAL_TABLE',
            'PRODUCT',
            'DEPOSIT_ACCOUNT',
            'GLG_ACCOUNT'
        ]
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            logger.info("📊 Table Record Counts:")
            logger.info("-" * 50)
            
            for table in tables_to_check:
                try:
                    cursor.execute(f'SELECT COUNT(*) FROM {table}')
                    count = cursor.fetchone()[0]
                    logger.info(f'{table:<20}: {count:>15,} records')
                except Exception as e:
                    logger.error(f'{table:<20}: ERROR - {str(e)[:50]}...')
            
            logger.info("\n" + "=" * 60)
            logger.info("🔍 Checking GLI_TRX_EXTRACT with GL account filters:")
            logger.info("-" * 60)
            
            # Check GLI_TRX_EXTRACT with different GL account patterns
            gl_patterns = ['13%', '130%', '131%', '132%', '133%', '134%', '135%']
            
            for pattern in gl_patterns:
                try:
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM GLI_TRX_EXTRACT gte
                        LEFT JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                        WHERE gl.EXTERNAL_GLACCOUNT LIKE '{pattern}'
                        AND gte.DC_AMOUNT > 0
                    """)
                    count = cursor.fetchone()[0]
                    logger.info(f'GL Account {pattern:<8}: {count:>15,} records')
                except Exception as e:
                    logger.error(f'GL Account {pattern:<8}: ERROR - {str(e)[:50]}...')
            
            logger.info("\n" + "=" * 60)
            logger.info("🔍 Checking other potential securities sources:")
            logger.info("-" * 60)
            
            # Check other potential sources
            other_queries = [
                ("PRODUCT with INVEST_FLAG", "SELECT COUNT(*) FROM PRODUCT WHERE INVEST_FLAG = 'Y'"),
                ("SECURITIES with data", "SELECT COUNT(*) FROM SECURITIES WHERE FK_PRODUCTID_PROD IS NOT NULL"),
                ("COLLATERAL_TABLE BS type", "SELECT COUNT(*) FROM COLLATERAL_TABLE WHERE RECORD_TYPE = 'BS'"),
                ("RSKCO_SECURITIES debt", "SELECT COUNT(*) FROM RSKCO_SECURITIES WHERE ISFUND = 0 AND ISINDEX = 0"),
            ]
            
            for desc, query in other_queries:
                try:
                    cursor.execute(query)
                    count = cursor.fetchone()[0]
                    logger.info(f'{desc:<25}: {count:>15,} records')
                except Exception as e:
                    logger.error(f'{desc:<25}: ERROR - {str(e)[:50]}...')
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error checking table counts: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 Securities Table Count Checker")
    print("=" * 50)
    
    success = check_table_counts()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ Table count check completed!")
    else:
        print("❌ Table count check failed - check logs above")