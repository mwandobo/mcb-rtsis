#!/usr/bin/env python3
"""
Test if we're losing data due to primary key constraints in other tables
"""

import logging
from db2_connection import DB2Connection
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_potential_data_loss():
    """Check if DB2 has more records than what we're storing in PostgreSQL"""
    
    config = Config()
    
    # Test queries to count records in DB2 for each table type
    test_queries = {
        'cash_information': """
            SELECT COUNT(*) FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
        """,
        'balances_bot': """
            SELECT COUNT(*) FROM GLI_TRX_EXTRACT AS gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT='100028000'
        """,
        'balance_with_other_banks': """
            SELECT COUNT(*) FROM GLI_TRX_EXTRACT as gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN('100050001')
        """,
        'balances_with_mnos': """
            SELECT COUNT(*) FROM GLI_TRX_EXTRACT gte
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
            WHERE gl.EXTERNAL_GLACCOUNT IN ('504080001','144000051','144000058','144000061','144000062')
        """,
        'asset_owned': """
            SELECT COUNT(*) FROM ASSET_MASTER AS M
            LEFT JOIN CURRENCY as CU ON CU.ID_CURRENCY = M.CURRENCY_ID
            LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = M.GL_ACCOUNT
        """
    }
    
    try:
        db2_conn = DB2Connection()
        
        print("=== DB2 vs PostgreSQL Record Count Comparison ===")
        print()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            for table_name, query in test_queries.items():
                try:
                    cursor.execute(query)
                    db2_count = cursor.fetchone()[0]
                    
                    print(f"📊 {table_name}:")
                    print(f"   DB2 Source: {db2_count:,} records available")
                    print(f"   Pipeline fetches: Limited by FETCH FIRST clauses")
                    print(f"   Potential for data loss: {'YES' if db2_count > 1000 else 'NO'}")
                    print()
                    
                except Exception as e:
                    print(f"❌ {table_name}: Query failed - {e}")
                    print()
        
        print("=== ANALYSIS ===")
        print("If DB2 has significantly more records than what the pipeline fetches,")
        print("the primary key constraints could be causing additional data loss")
        print("beyond the FETCH FIRST limitations.")
        
    except Exception as e:
        logger.error(f"DB2 connection failed: {e}")

if __name__ == "__main__":
    check_potential_data_loss()