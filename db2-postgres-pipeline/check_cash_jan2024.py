#!/usr/bin/env python3
"""
Check Cash Data Availability from January 2024
"""

import sys
import os
from datetime import datetime
import logging

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db2_connection import DB2Connection

def check_cash_data_availability():
    """Check what cash data is available from January 2024"""
    
    print("🔍 CHECKING CASH DATA AVAILABILITY FROM JAN 2024")
    print("=" * 60)
    
    try:
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            print("📊 Checking cash data from January 2024...")
            
            # Check data availability by month
            availability_query = """
            SELECT 
                YEAR(gte.TRN_DATE) as year,
                MONTH(gte.TRN_DATE) as month,
                COUNT(*) as record_count,
                MIN(gte.TRN_DATE) as earliest_date,
                MAX(gte.TRN_DATE) as latest_date,
                COUNT(DISTINCT gte.CURRENCY_SHORT_DES) as currency_count,
                SUM(CASE WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT ELSE 0 END) as tzs_total,
                SUM(CASE WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT ELSE 0 END) as usd_total
            FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
              AND gte.TRN_DATE >= '2024-01-01'
            GROUP BY YEAR(gte.TRN_DATE), MONTH(gte.TRN_DATE)
            ORDER BY year, month
            """
            
            cursor.execute(availability_query)
            results = cursor.fetchall()
            
            if results:
                print("\n📊 Cash Data Availability by Month:")
                print("-" * 100)
                print(f"{'Month':<12} {'Records':<10} {'Earliest':<12} {'Latest':<12} {'Currencies':<10} {'TZS Total':<15} {'USD Total':<12}")
                print("-" * 100)
                
                total_records = 0
                total_tzs = 0
                total_usd = 0
                
                for row in results:
                    year, month, count, earliest, latest, currencies, tzs_total, usd_total = row
                    month_name = f"{year}-{month:02d}"
                    total_records += count
                    total_tzs += tzs_total if tzs_total else 0
                    total_usd += usd_total if usd_total else 0
                    
                    print(f"{month_name:<12} {count:<10,} {str(earliest)[:10]:<12} {str(latest)[:10]:<12} {currencies:<10} {tzs_total:<15,.2f} {usd_total:<12,.2f}")
                
                print("-" * 100)
                print(f"{'TOTAL':<12} {total_records:<10,} {'':>34} {total_tzs:<15,.2f} {total_usd:<12,.2f}")
                print(f"\n✅ Found {total_records:,} cash records from January 2024 onwards")
                print(f"💰 Total TZS: {total_tzs:,.2f}")
                print(f"💵 Total USD: {total_usd:,.2f}")
            else:
                print("⚠️ No cash data found from January 2024")
                return False
                
            # Check GL accounts breakdown
            print(f"\n📋 Cash Categories Breakdown:")
            category_query = """
            SELECT 
                gl.EXTERNAL_GLACCOUNT,
                CASE 
                    WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
                    WHEN gl.EXTERNAL_GLACCOUNT IN ('101000010','101000015') THEN 'Cash in ATMs'
                    WHEN gl.EXTERNAL_GLACCOUNT IN ('101000004','101000011') THEN 'Cash in Teller'
                    WHEN gl.EXTERNAL_GLACCOUNT='101000007' THEN 'Other cash'
                    ELSE 'Unknown'
                END AS category,
                COUNT(*) as record_count,
                SUM(CASE WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT ELSE 0 END) as tzs_amount,
                SUM(CASE WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT ELSE 0 END) as usd_amount
            FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
              AND gte.TRN_DATE >= '2024-01-01'
            GROUP BY gl.EXTERNAL_GLACCOUNT
            ORDER BY record_count DESC
            """
            
            cursor.execute(category_query)
            categories = cursor.fetchall()
            
            print("-" * 80)
            print(f"{'GL Account':<12} {'Category':<15} {'Records':<10} {'TZS Amount':<15} {'USD Amount':<12}")
            print("-" * 80)
            
            for gl_account, category, count, tzs_amount, usd_amount in categories:
                print(f"{gl_account:<12} {category:<15} {count:<10,} {tzs_amount:<15,.2f} {usd_amount:<12,.2f}")
            
            # Check recent data (last 30 days)
            print(f"\n📅 Recent Cash Data (Last 30 Days):")
            recent_query = """
            SELECT 
                COUNT(*) as recent_count,
                MIN(gte.TRN_DATE) as earliest_recent,
                MAX(gte.TRN_DATE) as latest_recent
            FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
              AND gte.TRN_DATE >= CURRENT_DATE - 30 DAYS
            """
            
            cursor.execute(recent_query)
            recent_result = cursor.fetchone()
            
            if recent_result and recent_result[0] > 0:
                recent_count, earliest_recent, latest_recent = recent_result
                print(f"   Recent records: {recent_count:,}")
                print(f"   Date range: {earliest_recent} to {latest_recent}")
            else:
                print("   No recent cash data found")
            
            return True
            
    except Exception as e:
        print(f"❌ Error checking data availability: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    check_cash_data_availability()