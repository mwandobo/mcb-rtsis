#!/usr/bin/env python3
"""
Personal Data Streaming Pipeline Runner
"""

from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

def main():
    """Run the personal data streaming pipeline"""
    
    print("👤 PERSONAL DATA STREAMING PIPELINE RUNNER")
    print("=" * 60)
    print("📦 Batch size: 10 records per batch")
    print("🔄 Mode: Process ALL available personal data")
    print("👤 Table: personalData (camelCase)")
    print("🔑 Primary Key: customerIdentificationNumber")
    print("📋 Query: personal_data_information-v2.sql")
    print("=" * 60)
    
    # Initialize pipeline with batch size of 10
    pipeline = PersonalDataStreamingPipeline(10)
    
    try:
        print("🚀 Starting personal data streaming pipeline execution...")
        
        # Run the streaming pipeline
        total_processed = pipeline.run_streaming_pipeline()
        
        print(f"\n✅ Personal data pipeline completed successfully!")
        print(f"📊 Total records processed: {total_processed:,}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrupted by user")
        print("🛑 Stopping personal data streaming pipeline...")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()