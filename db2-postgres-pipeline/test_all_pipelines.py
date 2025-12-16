#!/usr/bin/env python3
"""
Test all three pipelines sequentially
"""

import psycopg2
from db2_connection import DB2Connection
from config import Config
from processors.cash_processor import CashProcessor, CashRecord
from processors.assets_processor import AssetsProcessor, AssetsRecord
from processors.bot_balances_processor import BotBalancesProcessor, BotBalancesRecord

def test_all_pipelines():
    """Test all pipelines sequentially"""
    config = Config()
    db2_conn = DB2Connection()
    
    # Initialize processors
    cash_processor = CashProcessor()
    assets_processor = AssetsProcessor()
    bot_processor = BotBalancesProcessor()
    
    print("🚀 Testing All Three Pipelines")
    print("=" * 50)
    
    try:
        # PostgreSQL connection
        conn_pg = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        # Test 1: Cash Information
        print("\n💰 Testing Cash Information Pipeline...")
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            simple_cash_query = """
            SELECT 
                gte.TRN_DATE,
                CURRENT_TIMESTAMP AS REPORTINGDATE,
                gte.FK_UNITCODETRXUNIT AS BRANCHCODE,
                'Cash in vault' AS CASHCATEGORY,
                gte.CURRENCY_SHORT_DES AS CURRENCY,
                gte.DC_AMOUNT AS ORGAMOUNT,
                CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT ELSE NULL END AS USDAMOUNT,
                CASE WHEN gte.CURRENCY_SHORT_DES='USD' THEN gte.DC_AMOUNT*2500 ELSE gte.DC_AMOUNT END AS TZSAMOUNT,
                gte.TRN_DATE AS TRANSACTIONDATE,
                gte.AVAILABILITY_DATE AS MATURITYDATE,
                CAST(0 AS DECIMAL(18,2)) AS ALLOWANCEPROBABLELOSS,
                CAST(0 AS DECIMAL(18,2)) AS BOTPROVISSION
            FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO=gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002')
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(simple_cash_query)
            rows = cursor.fetchall()
            
            print(f"  📊 Fetched {len(rows)} cash records")
            
            cursor_pg = conn_pg.cursor()
            cash_count = 0
            for row in rows:
                record = cash_processor.process_record(row, 'cash_information')
                if cash_processor.validate_record(record):
                    cash_processor.insert_to_postgres(record, cursor_pg)
                    conn_pg.commit()
                    cash_count += 1
                    print(f"    ✅ Cash: Branch {record.branch_code}, {record.amount_local:,.2f} {record.currency}")
            
            print(f"  💰 Inserted {cash_count} cash records")
        
        # Test 2: Assets
        print("\n🏢 Testing Assets Pipeline...")
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            simple_assets_query = """
            SELECT
                CURRENT_TIMESTAMP AS reportingDate,
                M.AQUISITION_DATE as acquisitionDate,
                CU.SHORT_DESCR as currency,
                'Movable' AS assetCategory,
                'Computer' AS assetType,
                M.AMOUNT AS orgCostValue,
                CASE WHEN CU.SHORT_DESCR = 'USD' THEN M.AMOUNT ELSE NULL END AS usdCostValue,
                CASE WHEN CU.SHORT_DESCR = 'USD' THEN M.AMOUNT * 2500 ELSE M.AMOUNT END AS tzsCostValue,
                0 as allowanceProbableLoss,
                0 as botProvision
            FROM ASSET_MASTER AS M
            LEFT JOIN CURRENCY as CU ON CU.ID_CURRENCY = M.CURRENCY_ID
            FETCH FIRST 3 ROWS ONLY
            """
            
            cursor.execute(simple_assets_query)
            rows = cursor.fetchall()
            
            print(f"  📊 Fetched {len(rows)} asset records")
            
            cursor_pg = conn_pg.cursor()
            assets_count = 0
            for row in rows:
                record = assets_processor.process_record(row, 'asset_owned')
                if assets_processor.validate_record(record):
                    assets_processor.insert_to_postgres(record, cursor_pg)
                    conn_pg.commit()
                    assets_count += 1
                    print(f"    ✅ Asset: {record.asset_type}, {record.org_cost_value:,.2f} {record.currency}")
            
            print(f"  🏢 Inserted {assets_count} asset records")
        
        # Test 3: BOT Balances
        print("\n🏛️ Testing BOT Balances Pipeline...")
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            bot_query = config.tables['balances_bot'].query.replace("FETCH FIRST 1000 ROWS ONLY", "FETCH FIRST 3 ROWS ONLY")
            
            cursor.execute(bot_query)
            rows = cursor.fetchall()
            
            print(f"  📊 Fetched {len(rows)} BOT balance records")
            
            cursor_pg = conn_pg.cursor()
            bot_count = 0
            for row in rows:
                record = bot_processor.process_record(row, 'balances_bot')
                if bot_processor.validate_record(record):
                    bot_processor.insert_to_postgres(record, cursor_pg)
                    conn_pg.commit()
                    bot_count += 1
                    print(f"    ✅ BOT: Account {record.account_number}, {record.org_amount:,.2f} {record.currency}")
            
            print(f"  🏛️ Inserted {bot_count} BOT balance records")
        
        conn_pg.close()
        
        print("\n🎉 All Three Pipelines Test Completed!")
        print("=" * 50)
        
        # Final check
        from check_data import check_data
        print("\n📊 Final Data Summary:")
        check_data()
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_all_pipelines()