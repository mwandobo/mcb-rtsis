#!/usr/bin/env python3
"""
Simple test for personal data pipeline - process just a few records
"""

from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

def main():
    """Test personal data pipeline with small batch"""
    
    print("🧪 TESTING PERSONAL DATA PIPELINE")
    print("=" * 50)
    print("📦 Testing with batch size: 5 records")
    print("🎯 Goal: Verify pipeline works correctly")
    print("=" * 50)
    
    # Initialize pipeline with small batch size for testing
    pipeline = PersonalDataStreamingPipeline(5)
    
    try:
        print("🚀 Starting test pipeline execution...")
        
        # Run the streaming pipeline
        total_processed = pipeline.run_streaming_pipeline()
        
        print(f"\n✅ Test completed successfully!")
        print(f"📊 Total records processed: {total_processed:,}")
        
        if total_processed > 0:
            print("🎉 Pipeline is working correctly!")
        else:
            print("⚠️ No records processed - check DB2 connection")
        
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()