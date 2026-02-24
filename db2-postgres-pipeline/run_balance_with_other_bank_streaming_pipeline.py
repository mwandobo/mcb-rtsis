#!/usr/bin/env python3
"""
Runner script for Balance with Other Bank Streaming Pipeline
"""

from balance_with_other_bank_streaming_pipeline import BalanceWithOtherBankStreamingPipeline

def main():
    print("=" * 60)
    print("Balance with Other Bank Streaming Pipeline")
    print("Based on: balance-with-other-bank-v1.sql")
    print("=" * 60)
    
    pipeline = BalanceWithOtherBankStreamingPipeline(batch_size=1000)
    
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
