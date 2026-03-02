#!/usr/bin/env python3
"""
Runner script for Inter-Bank Loan Receivable Streaming Pipeline
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from inter_bank_loan_receivable_streaming_pipeline import InterBankLoanReceivableStreamingPipeline

def main():
    print("🏦 RUNNING INTER-BANK LOAN RECEIVABLE STREAMING PIPELINE")
    print("=" * 70)
    print("📋 Features:")
    print("  - Uses inter-bank-loan-receivable-v4.sql")
    print("  - Currency conversion with fixing rates")
    print("  - Batch size: 500 records (optimized)")
    print("  - Producer + Consumer simultaneous streaming")
    print("=" * 70)
    
    pipeline = InterBankLoanReceivableStreamingPipeline(batch_size=500)
    
    try:
        pipeline.run_streaming_pipeline()
        print("\n" + "=" * 70)
        print("✅ INTER-BANK LOAN RECEIVABLE PIPELINE COMPLETED!")
        print("=" * 70)
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline stopped by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
