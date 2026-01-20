#!/usr/bin/env python3
"""
Debug agent transactions pipeline to see why it stops at 9 records
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db2_connection import DB2Connection
from processors.agent_transaction_processor import AgentTransactionProcessor
import logging

def debug_agent_transactions():
    """Debug the agent transactions processing"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    db2_conn = DB2Connection()
    processor = AgentTransactionProcessor()
    
    # Query with limit 10
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
    AND gte.TRN_DATE >= TIMESTAMP('2024-01-01 00:00:00')
    ORDER BY gte.TRN_DATE ASC
    FETCH FIRST 10 ROWS ONLY
    """
    
    try:
        logger.info("🔍 Debugging agent transactions processing...")
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            logger.info("📊 Executing query to fetch 10 records...")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            logger.info(f"✅ DB2 returned {len(rows)} raw records")
            
            if len(rows) < 10:
                logger.warning(f"⚠️ DB2 only returned {len(rows)} records, expected 10")
                logger.info("🔍 This means there are only {len(rows)} matching records in the source data")
            
            # Process each record
            valid_records = []
            invalid_records = []
            
            for i, row in enumerate(rows, 1):
                logger.info(f"\n📋 Processing record {i}:")
                logger.info(f"   Raw data: {row}")
                
                try:
                    # Process the record
                    record = processor.process_record(row, 'agentTransactions')
                    logger.info(f"   ✅ Processed: {record.transactionId}")
                    
                    # Validate the record
                    if processor.validate_record(record):
                        valid_records.append(record)
                        logger.info(f"   ✅ Valid record")
                    else:
                        invalid_records.append(record)
                        logger.info(f"   ❌ Invalid record - failed validation")
                        
                except Exception as e:
                    logger.error(f"   ❌ Processing error: {e}")
                    invalid_records.append(None)
            
            logger.info(f"\n📊 PROCESSING SUMMARY:")
            logger.info(f"   Raw records from DB2: {len(rows)}")
            logger.info(f"   Valid processed records: {len(valid_records)}")
            logger.info(f"   Invalid/failed records: {len(invalid_records)}")
            
            if len(valid_records) != len(rows):
                logger.warning(f"⚠️ Record count mismatch!")
                logger.warning(f"   Expected: {len(rows)} valid records")
                logger.warning(f"   Got: {len(valid_records)} valid records")
                logger.warning(f"   Lost: {len(rows) - len(valid_records)} records during processing")
            
            return len(rows), len(valid_records)
            
    except Exception as e:
        logger.error(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0

if __name__ == "__main__":
    print("🔍 AGENT TRANSACTIONS DEBUG")
    print("=" * 50)
    
    raw_count, valid_count = debug_agent_transactions()
    
    print(f"\n📊 FINAL RESULTS:")
    print(f"   DB2 raw records: {raw_count}")
    print(f"   Valid processed: {valid_count}")
    
    if raw_count < 10:
        print(f"\n💡 EXPLANATION:")
        print(f"   The pipeline stopped at {valid_count} because there are only")
        print(f"   {raw_count} matching records in the source database for the")
        print(f"   specified criteria (date >= 2024-01-01, GL accounts, agent mapping)")
    elif valid_count < raw_count:
        print(f"\n⚠️ ISSUE FOUND:")
        print(f"   {raw_count - valid_count} records were lost during processing")
        print(f"   Check validation logic or data quality issues")
    else:
        print(f"\n✅ PROCESSING OK:")
        print(f"   All {raw_count} records processed successfully")