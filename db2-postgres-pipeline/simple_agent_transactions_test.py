#!/usr/bin/env python3
"""
Simple test for agent transactions query
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db2_connection import DB2Connection
import logging

def test_agent_transactions_query():
    """Test the agent transactions query with a smaller limit"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    db2_conn = DB2Connection()
    
    # Simple query with small limit
    query = """
    SELECT
        CURRENT_TIMESTAMP AS reportingDate,
        al.AGENT_ID AS agentId,
        'active' AS agentStatus,
        gte.TRN_DATE AS transactionDate,
        VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
        TRIM(gte.FK_USRCODE) || '-' ||
        VARCHAR(gte.LINE_NUM) || '-' ||
        VARCHAR(gte.TRN_DATE) || '-' ||
        VARCHAR(gte.TRN_SNUM) AS transactionId,
        CASE
            WHEN gl.EXTERNAL_GLACCOUNT = '230000079' THEN 'Cash Deposit'
            WHEN gl.EXTERNAL_GLACCOUNT = '144000054' THEN 'Cash Withdraw'
        END AS transactionType,
        'Point of Sale' AS serviceChannel,
        NULL AS tillNumber,
        gte.CURRENCY_SHORT_DES AS currency,
        gte.DC_AMOUNT AS tzsAmount
    FROM GLI_TRX_EXTRACT gte
    INNER JOIN (
        SELECT DISTINCT
            CASE
                WHEN LENGTH(TRIM(TERMINAL_ID)) >= 8
                THEN SUBSTR(TRIM(TERMINAL_ID), LENGTH(TRIM(TERMINAL_ID)) - 7, 8)
                ELSE TRIM(TERMINAL_ID)
            END AS TERMINAL_ID_8,
            AGENT_ID
        FROM AGENTS_LIST
    ) al
        ON al.TERMINAL_ID_8 = TRIM(gte.TRX_USR)
    LEFT JOIN GLG_ACCOUNT gl
        ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
    WHERE gl.EXTERNAL_GLACCOUNT IN ('230000079', '144000054')
    AND gte.TRN_DATE >= DATE('2024-01-01')
    ORDER BY gte.TRN_DATE ASC
    FETCH FIRST 10 ROWS ONLY
    """
    
    try:
        logger.info("🏪 Testing agent transactions query...")
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            logger.info("📊 Executing query with 10 row limit...")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            logger.info(f"✅ Query executed successfully!")
            logger.info(f"📊 Found {len(rows)} records")
            
            if rows:
                logger.info("📋 Sample data:")
                for i, row in enumerate(rows[:3], 1):
                    logger.info(f"  {i}. Agent: {row[1]}, Date: {row[3]}, Type: {row[5]}")
                    logger.info(f"     Amount: {row[9]}, Currency: {row[8]}")
            else:
                logger.info("ℹ️ No records found - checking if tables exist...")
                
                # Check if AGENTS_LIST table exists
                cursor.execute("SELECT COUNT(*) FROM AGENTS_LIST FETCH FIRST 1 ROWS ONLY")
                agents_count = cursor.fetchone()[0]
                logger.info(f"📊 AGENTS_LIST has records: {agents_count > 0}")
                
                # Check GLI_TRX_EXTRACT
                cursor.execute("SELECT COUNT(*) FROM GLI_TRX_EXTRACT WHERE TRN_DATE >= DATE('2024-01-01') FETCH FIRST 1 ROWS ONLY")
                trx_count = cursor.fetchone()[0]
                logger.info(f"📊 GLI_TRX_EXTRACT has 2024+ records: {trx_count > 0}")
            
            return len(rows)
            
    except Exception as e:
        logger.error(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    print("🏪 SIMPLE AGENT TRANSACTIONS TEST")
    print("=" * 50)
    
    count = test_agent_transactions_query()
    
    print(f"\n✅ Test completed - found {count} records")
    if count > 0:
        print("🎉 Query is working! You can now run the full pipeline.")
    else:
        print("⚠️ No records found - check data availability or date range.")