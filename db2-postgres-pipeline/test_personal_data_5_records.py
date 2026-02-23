#!/usr/bin/env python3
"""
Test personal data pipeline with only 5 records
"""

from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

def main():
    print("\n" + "="*60)
    print("🧪 TESTING PERSONAL DATA PIPELINE - 5 RECORDS ONLY")
    print("="*60)
    print("📋 Configuration:")
    print("   - Batch size: 5 records")
    print("   - Query: v3 with corrected field mappings")
    print("   - Purpose: Test corrected pipeline")
    print("="*60)
    
    # Create pipeline with batch size of 5
    pipeline = PersonalDataStreamingPipeline(batch_size=5)
    
    try:
        print("\n🚀 Starting test pipeline...")
        
        # Run pipeline
        total = pipeline.run_streaming_pipeline()
        
        print("\n" + "="*60)
        print("✅ TEST COMPLETED")
        print("="*60)
        print(f"📊 Results:")
        print(f"   Total produced: {pipeline.total_produced:,}")
        print(f"   Total consumed: {pipeline.total_consumed:,}")
        print("="*60 + "\n")
        
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()