#!/usr/bin/env python3
"""
Mobile Banking Pipeline Runner - Executes the streaming pipeline
"""

from mobile_banking_streaming_pipeline import MobileBankingStreamingPipeline
import time

def main():
    """Run the mobile banking streaming pipeline"""
    
    print("🏦 MOBILE BANKING STREAMING PIPELINE RUNNER")
    print("=" * 60)
    print("📦 Batch size: 10 records per batch")
    print("🔄 Mode: Process ALL available mobile banking data")
    print("🏦 Table: mobileBanking (camelCase)")
    print("=" * 60)
    
    # Initialize pipeline with batch size of 10
    pipeline = MobileBankingStreamingPipeline(10)
    
    print("\n🚀 Starting mobile banking streaming pipeline execution...")
    start_time = time.time()
    
    try:
        # Run the streaming pipeline
        total_processed = pipeline.run_streaming_pipeline()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "=" * 60)
        print("✅ STREAMING MOBILE BANKING PIPELINE COMPLETED!")
        print(f"📊 Total records processed: {total_processed:,}")
        print(f"📦 Batch size used: 10")
        print(f"⏱️ Duration: {duration:.2f} seconds")
        print("🏦 Table: mobileBanking (camelCase)")
        print("📋 Fields: reportingDate, transactionDate, accountNumber, etc.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Mobile banking pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()