#!/usr/bin/env python3
"""
Run Cash Information Pipeline from January 2024
"""

import sys
import os
from datetime import datetime
import logging

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from production_cash_pipeline import ProductionCashPipeline
from pipeline_tracker import PipelineTracker

def run_cash_pipeline_from_jan2024():
    """Run cash pipeline starting from January 1, 2024"""
    
    print("💰 CASH INFORMATION PIPELINE - JANUARY 2024")
    print("=" * 60)
    print("📅 Starting from: January 1, 2024 00:00:00")
    print("🎯 Target: Process all cash transactions from Jan 2024")
    print("=" * 60)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('cash_pipeline_jan2024.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize tracker to show current status
        tracker = PipelineTracker()
        
        print("\n📊 Current Tracking Status:")
        tracker.show_all_tracking_info()
        
        # Set manual start timestamp for January 1, 2024
        manual_start = "2024-01-01 00:00:00"
        
        # Initialize pipeline with January 2024 start date
        pipeline = ProductionCashPipeline(
            manual_start_timestamp=manual_start,
            limit=5000  # Increased limit for historical data
        )
        
        logger.info(f"🚀 Starting cash pipeline from {manual_start}")
        
        # Run the pipeline
        pipeline.run_production_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ CASH PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        # Show final tracking status
        print("\n📊 Final Tracking Status:")
        tracker.show_all_tracking_info()
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def run_incremental_batches():
    """Run cash pipeline in incremental batches from January 2024"""
    
    print("💰 CASH PIPELINE - INCREMENTAL BATCHES FROM JAN 2024")
    print("=" * 60)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Define batch periods (monthly batches)
    batch_periods = [
        ("2024-01-01 00:00:00", "2024-01-31 23:59:59", "January 2024"),
        ("2024-02-01 00:00:00", "2024-02-29 23:59:59", "February 2024"),
        ("2024-03-01 00:00:00", "2024-03-31 23:59:59", "March 2024"),
        ("2024-04-01 00:00:00", "2024-04-30 23:59:59", "April 2024"),
        ("2024-05-01 00:00:00", "2024-05-31 23:59:59", "May 2024"),
        ("2024-06-01 00:00:00", "2024-06-30 23:59:59", "June 2024"),
        ("2024-07-01 00:00:00", "2024-07-31 23:59:59", "July 2024"),
        ("2024-08-01 00:00:00", "2024-08-31 23:59:59", "August 2024"),
        ("2024-09-01 00:00:00", "2024-09-30 23:59:59", "September 2024"),
        ("2024-10-01 00:00:00", "2024-10-31 23:59:59", "October 2024"),
        ("2024-11-01 00:00:00", "2024-11-30 23:59:59", "November 2024"),
        ("2024-12-01 00:00:00", "2024-12-31 23:59:59", "December 2024")
    ]
    
    total_processed = 0
    
    for start_date, end_date, period_name in batch_periods:
        print(f"\n🗓️ Processing {period_name}")
        print(f"📅 Period: {start_date} to {end_date}")
        print("-" * 40)
        
        try:
            # Initialize pipeline for this batch
            pipeline = ProductionCashPipeline(
                manual_start_timestamp=start_date,
                limit=10000  # Higher limit for monthly batches
            )
            
            logger.info(f"🚀 Processing {period_name}")
            
            # Run pipeline for this period
            pipeline.run_production_pipeline()
            
            print(f"✅ {period_name} completed")
            
        except Exception as e:
            logger.error(f"❌ Failed processing {period_name}: {e}")
            continue
    
    print(f"\n🎉 All batches completed!")

def check_cash_data_availability():
    """Check what cash data is available from January 2024"""
    
    print("🔍 CHECKING CASH DATA AVAILABILITY FROM JAN 2024")
    print("=" * 60)
    
    try:
        from db2_connection import DB2Connection
        
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
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
                print("-" * 80)
                print(f"{'Month':<12} {'Records':<10} {'Earliest':<12} {'Latest':<12} {'TZS Total':<15} {'USD Total':<12}")
                print("-" * 80)
                
                total_records = 0
                for row in results:
                    year, month, count, earliest, latest, currencies, tzs_total, usd_total = row
                    month_name = f"{year}-{month:02d}"
                    total_records += count
                    
                    print(f"{month_name:<12} {count:<10,} {str(earliest)[:10]:<12} {str(latest)[:10]:<12} {tzs_total:<15,.2f} {usd_total:<12,.2f}")
                
                print("-" * 80)
                print(f"{'TOTAL':<12} {total_records:<10,}")
                print(f"\n✅ Found {total_records:,} cash records from January 2024 onwards")
            else:
                print("⚠️ No cash data found from January 2024")
                
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
                    ELSE 'Other cash'
                END AS category,
                COUNT(*) as record_count,
                SUM(CASE WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN gte.DC_AMOUNT ELSE 0 END) as tzs_amount
            FROM GLI_TRX_EXTRACT gte 
            JOIN GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID 
            WHERE gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015','101000011')
              AND gte.TRN_DATE >= '2024-01-01'
            GROUP BY gl.EXTERNAL_GLACCOUNT
            ORDER BY record_count DESC
            """
            
            cursor.execute(category_query)
            categories = cursor.fetchall()
            
            print("-" * 60)
            print(f"{'GL Account':<12} {'Category':<15} {'Records':<10} {'TZS Amount':<15}")
            print("-" * 60)
            
            for gl_account, category, count, amount in categories:
                print(f"{gl_account:<12} {category:<15} {count:<10,} {amount:<15,.2f}")
            
    except Exception as e:
        print(f"❌ Error checking data availability: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function with options"""
    
    print("💰 CASH INFORMATION PIPELINE - JANUARY 2024")
    print("=" * 60)
    print("Choose an option:")
    print("1. Check data availability from Jan 2024")
    print("2. Run pipeline from Jan 1, 2024 (single run)")
    print("3. Run incremental monthly batches from Jan 2024")
    print("4. Exit")
    print("=" * 60)
    
    while True:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            check_cash_data_availability()
            break
        elif choice == "2":
            run_cash_pipeline_from_jan2024()
            break
        elif choice == "3":
            run_incremental_batches()
            break
        elif choice == "4":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")

if __name__ == "__main__":
    main()