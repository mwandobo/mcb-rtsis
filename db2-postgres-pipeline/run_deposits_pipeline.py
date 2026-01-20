#!/usr/bin/env python3
"""
Run Deposits Streaming Pipeline
"""

from deposits_streaming_pipeline import DepositsStreamingPipeline

def main():
    """Main function to run deposits streaming pipeline"""
    
    print("🏦 DEPOSITS STREAMING PIPELINE RUNNER")
    print("=" * 60)
    print("📦 Batch size: 10 records per batch")
    print("🔄 Mode: Process ALL available deposits data")
    print("🏦 Table: deposits (camelCase)")
    print("🔑 Primary Key: transactionUniqueRef (unique)")
    print("📋 Query: deposits.sql with ROW_NUMBER() logic")
    print("=" * 60)
    
    # Initialize pipeline with batch size of 10
    pipeline = DepositsStreamingPipeline(batch_size=10)
    
    try:
        print("🚀 Starting deposits streaming pipeline execution...")
        
        # Run the streaming pipeline
        total_processed = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ DEPOSITS STREAMING PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"📊 Total records processed: {total_processed:,}")
        print("🎉 All deposits data has been processed!")
        print("🔍 Features used:")
        print("  ✓ Streaming architecture (Producer + Consumer)")
        print("  ✓ Real-time processing")
        print("  ✓ Batch processing (10 records per batch)")
        print("  ✓ camelCase naming (table + fields)")
        print("  ✓ Unique transactionUniqueRef values")
        print("  ✓ ROW_NUMBER() for data deduplication")
        print("  ✓ Cursor-based pagination")
        print("  ✓ RabbitMQ message queue")
        print("  ✓ PostgreSQL with indexes")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrupted by user")
        print("🛑 Stopping deposits streaming pipeline...")
        
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()