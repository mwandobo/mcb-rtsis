#!/usr/bin/env python3
"""
Runner script for Balances with MNOs Streaming Pipeline
"""

from balances_with_mnos_streaming_pipeline import BalancesWithMnosStreamingPipeline

def main():
    print("=" * 60)
    print("Balances with MNOs Streaming Pipeline")
    print("Based on: balances-with-mnos.sql")
    print("=" * 60)
    
    pipeline = BalancesWithMnosStreamingPipeline(batch_size=1000)
    
    try:
        pipeline.run_streaming_pipeline()
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n\nPipeline failed with error: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
