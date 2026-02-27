#!/usr/bin/env python3
"""
Run Cash Information Streaming Pipeline
Based on cash-information.sql
"""

from cash_streaming_pipeline_simple import CashStreamingPipeline

def main():
    """Main function to run the cash information streaming pipeline"""
    
    print("=" * 60)
    print("Cash Information Streaming Pipeline")
    print("Based on: cash-information.sql")
    print("=" * 60)
    
    # Create pipeline with batch size (smaller batch for faster processing)
    pipeline = CashStreamingPipeline(batch_size=500)
    
    try:
        # Run the streaming pipeline
        pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n\nPipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
