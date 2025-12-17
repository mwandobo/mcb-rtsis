#!/usr/bin/env python3
"""
Check Data Availability in GLI_TRX_EXTRACT
"""

import logging
from db2_connection import DB2Connection
from config import Config

def check_data_availability():
    """Check what data is available in GLI_TRX_EXTRACT table"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 Checking Data Availability in GLI_TRX_EXTRACT")
    logger.info("=" * 60)
    
    try:
        config = Config()
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check 1: Total count in GLI_TRX_EXTRACT
            logger.info("📊 Checking total records in GLI_TRX_EXTRACT...")
            cursor.execute("SELECT COUNT(*) FROM GLI_TRX_EXTRACT")
            total_count = cursor.fetchone()[0]
            logger.info(f"   Total records: {total_count:,}")
            
            if total_count == 0:
                logger.info("❌ No data in GLI_TRX_EXTRACT table!")
                return
            
            # Check 2: Date range of data
            logger.info("\n📅 Checking date range of data...")
            cursor.execute("""
                SELECT 
                    MIN(TRN_DATE) as earliest_date,
                    MAX(TRN_DATE) as latest_date,
                    MIN(TMSTAMP) as earliest_timestamp,
                    MAX(TMSTAMP) as latest_timestamp
                FROM GLI_TRX_EXTRACT
            """)
            date_range = cursor.fetchone()
            logger.info(f"   TRN_DATE range: {date_range[0]} to {date_range[1]}")
            logger.info(f"   TMSTAMP range: {date_range[2]} to {date_range[3]}")
            
            # Check 3: Records with cash-related GL accounts
            logger.info("\n💰 Checking cash-related records...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM GLI_TRX_EXTRACT gte 
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
            """)
            cash_count = cursor.fetchone()[0]
            logger.info(f"   Cash-related records: {cash_count:,}")
            
            if cash_count == 0:
                logger.info("❌ No cash-related records found!")
                
                # Check what GL accounts are available
                logger.info("\n🔍 Checking available GL accounts...")
                cursor.execute("""
                    SELECT DISTINCT gl.EXTERNAL_GLACCOUNT, COUNT(*) as record_count
                    FROM GLI_TRX_EXTRACT gte 
                    JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                    GROUP BY gl.EXTERNAL_GLACCOUNT
                    ORDER BY record_count DESC
                    FETCH FIRST 10 ROWS ONLY
                """)
                gl_accounts = cursor.fetchall()
                logger.info("   Top 10 GL accounts by record count:")
                for account, count in gl_accounts:
                    logger.info(f"     {account}: {count:,} records")
                
                return
            
            # Check 4: Recent cash records (last 30 days)
            logger.info("\n📅 Checking recent cash records (last 30 days)...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM GLI_TRX_EXTRACT gte 
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
                AND gte.TMSTAMP >= CURRENT_DATE - 30 DAYS
            """)
            recent_count = cursor.fetchone()[0]
            logger.info(f"   Recent cash records (30 days): {recent_count:,}")
            
            # Check 5: Sample cash records
            logger.info("\n📋 Sample cash records:")
            cursor.execute("""
                SELECT 
                    gte.TMSTAMP,
                    gte.TRN_DATE,
                    gte.FK_UNITCODETRXUNIT AS BRANCHCODE,
                    gl.EXTERNAL_GLACCOUNT,
                    gte.CURRENCY_SHORT_DES,
                    gte.DC_AMOUNT
                FROM GLI_TRX_EXTRACT gte 
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
                ORDER BY gte.TMSTAMP DESC
                FETCH FIRST 5 ROWS ONLY
            """)
            samples = cursor.fetchall()
            
            if samples:
                for i, record in enumerate(samples, 1):
                    logger.info(f"   {i}. {record[0]} | TRN_DATE: {record[1]} | Branch: {record[2]} | GL: {record[3]} | {record[5]:,.2f} {record[4]}")
            else:
                logger.info("   No sample records found")
            
            # Check 6: Test our exact query
            logger.info("\n🧪 Testing our exact cash query...")
            cash_query = """
                SELECT 
                    gte.TMSTAMP,
                    gte.TRN_DATE,
                    CURRENT_TIMESTAMP AS REPORTINGDATE,
                    gte.FK_UNITCODETRXUNIT AS BRANCHCODE,
                    CASE 
                        WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                        WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('101000010','101000015') THEN 'Cash in ATMs'
                        WHEN gl.EXTERNAL_GLACCOUNT IN ('101000004','101000011') THEN 'Cash in Teller'
                        ELSE 'Other cash'
                    END AS CASHCATEGORY,
                    gte.CURRENCY_SHORT_DES AS CURRENCY,
                    gte.DC_AMOUNT AS ORGAMOUNT
                FROM GLI_TRX_EXTRACT gte 
                JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
                WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
                AND gte.TMSTAMP >= CURRENT_DATE - 30 DAYS
                ORDER BY gte.TMSTAMP DESC
                FETCH FIRST 10 ROWS ONLY
            """
            
            cursor.execute(cash_query)
            test_results = cursor.fetchall()
            logger.info(f"   Query returned {len(test_results)} records")
            
            if test_results:
                logger.info("   Sample results:")
                for i, record in enumerate(test_results[:3], 1):
                    logger.info(f"     {i}. {record[0]} | {record[3]} | {record[4]} | {record[6]:,.2f} {record[5]}")
                
                logger.info(f"\n✅ Pipeline should work! Found {len(test_results)} records in last 30 days")
                
                # Suggest running with 30-day lookback
                logger.info("\n💡 Suggestion: Run pipeline with 30-day lookback to get data")
                
            else:
                logger.info("   No records found with current query")
                logger.info("\n💡 Suggestion: Check if this is a test database with limited data")
            
    except Exception as e:
        logger.error(f"❌ Data check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_data_availability()