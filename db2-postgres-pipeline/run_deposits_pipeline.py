#!/usr/bin/env python3
"""
Run deposits streaming pipeline
"""

from deposits_streaming_pipeline import DepositsStreamingPipeline

def main():
    """Run the deposits pipeline"""
    print("🏦 DEPOSITS STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Uses deposits-v1.sql query")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Complex location mapping")
    print("  - PROFITS_ACCOUNT joins")
    print("  - Currency conversion with fixing_rate")
    print("  - Batch size: 500 records per batch")
    print("  - 37 fields from deposits-v1.sql")
    print("=" * 60)
    
    pipeline = DepositsStreamingPipeline(batch_size=500)
    
    try:
        pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ DEPOSITS STREAMING PIPELINE COMPLETED!")
        print("🔍 Key features used:")
        print("  - Full deposits-v1.sql implementation")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Complex location lookups")
        print("  - Currency conversion")
        print("  - Memory efficient")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Deposits pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()