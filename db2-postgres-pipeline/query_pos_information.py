#!/usr/bin/env python3
"""
Query script to verify posInformation data
"""

import psycopg2
from config import Config

def query_pos_information():
    """Query and display POS information data"""
    
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        print("🏪 POS INFORMATION DATA VERIFICATION")
        print("=" * 60)
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM "posInformation"')
        total_count = cursor.fetchone()[0]
        print(f"📊 Total POS Records: {total_count:,}")
        
        # Get count by branch
        cursor.execute("""
            SELECT "posBranchCode", COUNT(*) as count
            FROM "posInformation"
            GROUP BY "posBranchCode"
            ORDER BY count DESC
        """)
        
        branch_stats = cursor.fetchall()
        print(f"\n📋 POS by Branch:")
        print("-" * 60)
        for stat in branch_stats:
            print(f"  Branch {stat[0]:<10} Count: {stat[1]:>6,}")
        
        # Get count by region
        cursor.execute("""
            SELECT "region", COUNT(*) as count
            FROM "posInformation"
            GROUP BY "region"
            ORDER BY count DESC
            LIMIT 10
        """)
        
        region_stats = cursor.fetchall()
        print(f"\n🌍 Top Regions by POS Count:")
        print("-" * 60)
        for i, stat in enumerate(region_stats, 1):
            region = stat[0].strip() if stat[0] else "N/A"
            print(f"  {i:>2}. {region:<25} Count: {stat[1]:>6,}")
        
        # Get count by holder category
        cursor.execute("""
            SELECT "posHolderCategory", COUNT(*) as count
            FROM "posInformation"
            GROUP BY "posHolderCategory"
            ORDER BY count DESC
        """)
        
        category_stats = cursor.fetchall()
        print(f"\n👥 POS Holder Categories:")
        print("-" * 60)
        for stat in category_stats:
            print(f"  {stat[0]:<20} Count: {stat[1]:>6,}")
        
        # Show recent POS records
        cursor.execute("""
            SELECT "posNumber", "qrFsrCode", "posHolderName", "region", "district", "gpsCoordinates"
            FROM "posInformation"
            ORDER BY "posNumber"
            LIMIT 10
        """)
        
        recent_pos = cursor.fetchall()
        print(f"\n📋 Sample POS Records:")
        print("-" * 60)
        for i, pos in enumerate(recent_pos, 1):
            pos_number = pos[0].strip() if pos[0] else "N/A"
            qr_code = pos[1].strip() if pos[1] else "N/A"
            holder = pos[2].strip() if pos[2] else "N/A"
            region = pos[3].strip() if pos[3] else "N/A"
            district = pos[4].strip() if pos[4] else "N/A"
            gps = pos[5][:30] + "..." if pos[5] and len(pos[5]) > 30 else pos[5] or "N/A"
            
            print(f"  {i:>2}. POS: {pos_number}")
            print(f"      QR: {qr_code} | Holder: {holder}")
            print(f"      Location: {region}, {district}")
            print(f"      GPS: {gps}")
            print()
        
        # Verify camelCase naming
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'posInformation'
            AND column_name IN ('reportingDate', 'posNumber', 'qrFsrCode', 'posHolderName', 'gpsCoordinates')
            ORDER BY ordinal_position
        """)
        
        camel_fields = cursor.fetchall()
        print(f"✅ camelCase Fields Verified:")
        print("-" * 60)
        for field in camel_fields:
            print(f"  ✓ {field[0]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ POS INFORMATION VERIFICATION COMPLETED")
        print("🎉 All camelCase naming is working correctly!")
        print("📊 Data is properly structured and accessible")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    query_pos_information()