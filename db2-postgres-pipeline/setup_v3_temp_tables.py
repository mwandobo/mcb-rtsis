#!/usr/bin/env python3
"""
Create session temporary tables for v3 query CTEs
This needs to be done ONCE before running the pipeline
"""

from db2_connection import DB2Connection

def main():
    print("\n" + "="*60)
    print("🔧 CREATING SESSION TEMP TABLES FOR V3 QUERY")
    print("="*60)
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            print("\n1️⃣ Creating district_wards temp table...")
            cursor.execute("""
                DECLARE GLOBAL TEMPORARY TABLE district_wards AS (
                    SELECT DISTINCT DISTRICT,
                                    WARD,
                                    ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn,
                                    COUNT(*) OVER (PARTITION BY DISTRICT) AS total_wards
                    FROM bank_location_lookup_v2
                ) WITH DATA ON COMMIT PRESERVE ROWS NOT LOGGED
            """)
            print("   ✅ district_wards created")
            
            print("\n2️⃣ Creating region_districts temp table...")
            cursor.execute("""
                DECLARE GLOBAL TEMPORARY TABLE region_districts AS (
                    SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) AS rn,
                           COUNT(*) OVER (PARTITION BY REGION) AS total_districts
                    FROM bank_location_lookup_v2
                    GROUP BY REGION, DISTRICT
                ) WITH DATA ON COMMIT PRESERVE ROWS NOT LOGGED
            """)
            print("   ✅ region_districts created")
            
            conn.commit()
            
            # Test the temp tables
            print("\n3️⃣ Testing temp tables...")
            cursor.execute("SELECT COUNT(*) FROM SESSION.district_wards")
            count1 = cursor.fetchone()[0]
            print(f"   district_wards: {count1:,} rows")
            
            cursor.execute("SELECT COUNT(*) FROM SESSION.region_districts")
            count2 = cursor.fetchone()[0]
            print(f"   region_districts: {count2:,} rows")
            
            print("\n" + "="*60)
            print("✅ TEMP TABLES CREATED SUCCESSFULLY")
            print("="*60)
            print("⚠️  NOTE: These tables exist only for this DB2 session")
            print("   The pipeline must use the SAME connection to access them")
            print("="*60 + "\n")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
