#!/usr/bin/env python3
"""
Run Personal Data OPTIMIZED Streaming Pipeline with 1000 batch size
"""

from personal_data_streaming_pipeline_optimized import PersonalDataOptimizedStreamingPipeline

def main():
    """Run the optimized personal data streaming pipeline with maximum performance"""
    print("🚀 RUNNING PERSONAL DATA OPTIMIZED STREAMING PIPELINE")
    print("=" * 70)
    print("📋 Configuration:")
    print("  - Batch size: 1000 records (MAXIMUM PERFORMANCE)")
    print("  - Uses personal-data-optimized.sql with CTEs")
    print("  - ⚡ PERFORMANCE OPTIMIZED query structure")
    print("  - Producer + Consumer simultaneous streaming")
    print("  - Queue: personal_data_optimized_queue")
    print("=" * 70)
    
    # Create pipeline with maximum batch size
    pipeline = PersonalDataOptimizedStreamingPipeline(1000)
    
    try:
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 70)
        print("✅ OPTIMIZED PIPELINE COMPLETED!")
        print(f"📊 Total records processed: {count:,}")
        print("🔍 Performance Features:")
        print("  - CTEs optimization working")
        print("  - 1000 records per batch")
        print("  - Minimal database round trips")
        print("  - Real-time streaming processing")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Optimized pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()