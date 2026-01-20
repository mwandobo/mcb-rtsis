#!/usr/bin/env python3
"""
Debug POS pipeline to see why only 236 out of 521 records were processed
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db2_connection import DB2Connection
from processors.pos_information_processor import PosInformationProcessor
import logging
import psycopg2
from config import Config

def debug_pos_pipeline():
    """Debug the POS pipeline processing"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    db2_conn = DB2Connection()
    processor = PosInformationProcessor()
    config = Config()
    
    print("🔍 POS PIPELINE DEBUG ANALYSIS")
    print("=" * 60)
    
    # 1. Check total count in DB2
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            count_query = """
            SELECT COUNT(*) as total_count
            FROM AGENT_TERMINAL at
            JOIN (SELECT DISTINCT RIGHT(TRIM(TERMINAL_ID), 8) AS TERMINAL_ID_8, GPS
                  FROM AGENTS_LIST) al
                 ON al.TERMINAL_ID_8 = TRIM(at.FK_USRCODE)
            """
            
            cursor.execute(count_query)
            total_db2 = cursor.fetchone()[0]
            print(f"📊 Total records in DB2: {total_db2:,}")
            
    except Exception as e:
        print(f"❌ DB2 count error: {e}")
        total_db2 = 0
    
    # 2. Check total count in PostgreSQL
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM "posInformation"')
        total_pg = cursor.fetchone()[0]
        print(f"📊 Total records in PostgreSQL: {total_pg:,}")
        conn.close()
        
    except Exception as e:
        print(f"❌ PostgreSQL count error: {e}")
        total_pg = 0
    
    # 3. Check for validation failures
    print(f"\n🔍 ANALYZING POTENTIAL ISSUES:")
    print(f"   Missing records: {total_db2 - total_pg:,}")
    
    # 4. Test processing a sample of records
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get first 50 records to test processing
            test_query = """
            SELECT * FROM (
                SELECT 
                    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')    AS reportingDate,
                    201                                                  AS posBranchCode,
                    at.FK_USRCODE                                        AS posNumber,
                    'FSR-' || CAST(at.FK_USRCODE AS VARCHAR(10))         AS qrFsrCode,
                    'Bank Agent'                                         AS posHolderCategory,
                    'Selcom'                                             AS posHolderName,
                    null                                                 AS posHolderNin,
                    '103-847-451'                                        AS posHolderTin,
                    NULL                                                 AS postalCode,
                    COALESCE(region_lkp.BOT_REGION, 'N/A')               AS region,
                    COALESCE(district_lkp.BOT_DISTRICT, 'N/A')           AS district,
                    COALESCE(ward_lkp.BOT_WARD, 'N/A')                   AS ward,
                    'N/A'                                                AS street,
                    'N/A'                                                AS houseNumber,
                    al.GPS                                               AS gpsCoordinates,
                    '230000070'                                          AS linkedAccount,
                    VARCHAR_FORMAT(at.INSERTION_TMSTAMP, 'DDMMYYYYHHMM') AS issueDate,
                    NULL                                                 AS returnDate,
                    
                    ROW_NUMBER() OVER (ORDER BY at.FK_USRCODE ASC) AS rn
                    
                FROM AGENT_TERMINAL at
                JOIN (SELECT DISTINCT RIGHT(TRIM(TERMINAL_ID), 8) AS TERMINAL_ID_8, GPS
                      FROM AGENTS_LIST) al
                     ON al.TERMINAL_ID_8 = TRIM(at.FK_USRCODE)
                LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                                  bl.REGION                             AS BOT_REGION,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                                      ORDER BY
                                          CASE
                                              WHEN UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION)) THEN 1
                                              WHEN UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%' THEN 2
                                              ELSE 99
                                              END,
                                          LENGTH(TRIM(bl.REGION)) DESC
                                      )                                 AS rn
                           FROM AGENTS_LIST al
                           JOIN BANK_LOCATION_LOOKUP_V2 bl
                                ON UPPER(TRIM(al.REGION)) = UPPER(TRIM(bl.REGION))
                                    OR (
                                       UPPER(TRIM(al.REGION)) LIKE UPPER(TRIM(bl.REGION)) || '%'
                                           AND LENGTH(TRIM(bl.REGION)) >= 4
                                       )) region_lkp
                          ON region_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                              AND region_lkp.rn = 1
                LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                                  bl.DISTRICT                           AS BOT_DISTRICT,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                                      ORDER BY
                                          CASE
                                              WHEN UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT)) THEN 1
                                              WHEN UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                                  AND LENGTH(TRIM(bl.DISTRICT)) >= 4 THEN 2
                                              ELSE 99
                                              END,
                                          LENGTH(TRIM(bl.DISTRICT)) DESC
                                      )                                 AS rn
                           FROM AGENTS_LIST al
                           JOIN BANK_LOCATION_LOOKUP_V2 bl
                                ON (
                                    UPPER(TRIM(al.DISTRICT)) = UPPER(TRIM(bl.DISTRICT))
                                        OR (
                                        UPPER(TRIM(al.DISTRICT)) LIKE UPPER(TRIM(bl.DISTRICT)) || '%'
                                            AND LENGTH(TRIM(bl.DISTRICT)) >= 4
                                        )
                                    )
                           WHERE TRIM(al.DISTRICT) IS NOT NULL
                             AND TRIM(al.DISTRICT) <> '') district_lkp
                          ON district_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                              AND district_lkp.rn = 1
                LEFT JOIN (SELECT TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8)) AS TERMINAL_KEY,
                                  bl.WARD                               AS BOT_WARD,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY TRIM(RIGHT(RTRIM(al.TERMINAL_ID), 8))
                                      ORDER BY
                                          CASE
                                              WHEN UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD)) THEN 1
                                              WHEN UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                                  AND LENGTH(TRIM(bl.WARD)) >= 4 THEN 2
                                              ELSE 99
                                              END,
                                          LENGTH(TRIM(bl.WARD)) DESC
                                      )                                 AS rn
                           FROM AGENTS_LIST al
                           JOIN BANK_LOCATION_LOOKUP_V2 bl
                                ON (
                                    UPPER(TRIM(al.LOCATION)) = UPPER(TRIM(bl.WARD))
                                        OR (
                                        UPPER(TRIM(al.LOCATION)) LIKE UPPER(TRIM(bl.WARD)) || '%'
                                            AND LENGTH(TRIM(bl.WARD)) >= 4
                                        )
                                    )
                           WHERE TRIM(al.LOCATION) IS NOT NULL
                             AND TRIM(al.LOCATION) <> '') ward_lkp
                          ON ward_lkp.TERMINAL_KEY = TRIM(at.FK_USRCODE)
                              AND ward_lkp.rn = 1
            ) numbered_results
            ORDER BY rn
            FETCH FIRST 50 ROWS ONLY
            """
            
            cursor.execute(test_query)
            rows = cursor.fetchall()
            
            print(f"\n🧪 TESTING SAMPLE PROCESSING:")
            print(f"   Sample size: {len(rows)} records")
            
            valid_count = 0
            invalid_count = 0
            duplicate_pos_numbers = set()
            
            for i, row in enumerate(rows, 1):
                try:
                    # Remove row number column
                    row_without_rn = row[:-1]
                    record = processor.process_record(row_without_rn, 'posInformation')
                    
                    # Check for duplicates
                    if record.posNumber in duplicate_pos_numbers:
                        print(f"   ⚠️ Duplicate posNumber found: {record.posNumber}")
                    else:
                        duplicate_pos_numbers.add(record.posNumber)
                    
                    if processor.validate_record(record):
                        valid_count += 1
                    else:
                        invalid_count += 1
                        print(f"   ❌ Invalid record {i}: posNumber={record.posNumber}")
                        
                except Exception as e:
                    invalid_count += 1
                    print(f"   ❌ Processing error {i}: {e}")
            
            print(f"   ✅ Valid records: {valid_count}")
            print(f"   ❌ Invalid records: {invalid_count}")
            print(f"   🔄 Success rate: {(valid_count/len(rows)*100):.1f}%")
            
    except Exception as e:
        print(f"❌ Sample processing error: {e}")
    
    # 5. Check for duplicate posNumbers in PostgreSQL
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT "posNumber", COUNT(*) as count
            FROM "posInformation"
            GROUP BY "posNumber"
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        
        duplicates = cursor.fetchall()
        
        if duplicates:
            print(f"\n⚠️ DUPLICATE posNumbers in PostgreSQL:")
            for dup in duplicates:
                print(f"   posNumber: {dup[0]} appears {dup[1]} times")
        else:
            print(f"\n✅ No duplicate posNumbers found in PostgreSQL")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Duplicate check error: {e}")
    
    # 6. Summary and recommendations
    print(f"\n📋 SUMMARY:")
    print(f"   DB2 Total: {total_db2:,}")
    print(f"   PostgreSQL: {total_pg:,}")
    print(f"   Missing: {total_db2 - total_pg:,}")
    print(f"   Success Rate: {(total_pg/total_db2*100):.1f}%")
    
    if total_pg < total_db2:
        print(f"\n💡 POSSIBLE CAUSES:")
        print(f"   1. Validation failures (empty required fields)")
        print(f"   2. Duplicate posNumber conflicts (PRIMARY KEY)")
        print(f"   3. Processing errors during transformation")
        print(f"   4. RabbitMQ message processing issues")
        print(f"   5. Pipeline stopped before completion")

if __name__ == "__main__":
    debug_pos_pipeline()