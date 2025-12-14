#!/usr/bin/env python3
"""
Test assets pipeline only
"""

import psycopg2
from db2_connection import DB2Connection
from config import Config
from processors.assets_processor import AssetsProcessor, AssetsRecord

def test_assets_pipeline():
    """Test assets pipeline"""
    config = Config()
    db2_conn = DB2Connection()
    assets_processor = AssetsProcessor()
    
    try:
        # Step 1: Fetch from DB2
        print("🏢 Fetching assets from DB2...")
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Use simple assets query
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
            FETCH FIRST 5 ROWS ONLY
            """
            
            cursor.execute(simple_assets_query)
            rows = cursor.fetchall()
            
            print(f"✅ Fetched {len(rows)} asset records")
            
            if not rows:
                print("⚠️ No asset records found")
                return
            
            # Step 2: Process records
            print("🔄 Processing asset records...")
            records = []
            for row in rows:
                try:
                    record = assets_processor.process_record(row, 'asset_owned')
                    if assets_processor.validate_record(record):
                        records.append(record)
                        print(f"  ✅ Processed: {record.asset_type}, {record.org_cost_value:,.2f} {record.currency}")
                    else:
                        print(f"  ❌ Invalid record: {row}")
                except Exception as e:
                    print(f"  ❌ Processing error: {e}")
            
            # Step 3: Insert to PostgreSQL
            if records:
                print(f"\n💾 Inserting {len(records)} records to PostgreSQL...")
                conn_pg = psycopg2.connect(
                    host=config.database.pg_host,
                    port=config.database.pg_port,
                    database=config.database.pg_database,
                    user=config.database.pg_user,
                    password=config.database.pg_password
                )
                
                cursor_pg = conn_pg.cursor()
                
                for record in records:
                    try:
                        assets_processor.insert_to_postgres(record, cursor_pg)
                        conn_pg.commit()
                        print(f"  ✅ Inserted: {record.asset_type}")
                    except Exception as e:
                        print(f"  ❌ Insert error: {e}")
                        conn_pg.rollback()
                
                cursor_pg.close()
                conn_pg.close()
                
                print(f"\n🎉 Assets pipeline test completed!")
            
    except Exception as e:
        print(f"❌ Assets pipeline test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_assets_pipeline()