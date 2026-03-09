#!/usr/bin/env python3
"""
Runner script for Account Information Streaming Pipeline
"""

from account_information_streaming_pipeline import AccountInformationStreamingPipeline

if __name__ == "__main__":
    pipeline = AccountInformationStreamingPipeline(
        batch_size=1000,           # Fetch 1000 records per batch from DB2
        consumer_batch_size=100    # Insert 100 records per batch to PostgreSQL
    )
    
    result = pipeline.run_streaming_pipeline()
    
    print("\n" + "=" * 60)
    print("Pipeline execution completed!")
    print(f"Total produced: {result['total_produced']:,}")
    print(f"Total consumed: {result['total_consumed']:,}")
    print(f"Total time: {result['total_time']:.1f} seconds")
    print(f"Average rate: {result['avg_rate']:.1f} records/second")
    print("=" * 60)
