#!/usr/bin/env python3
"""
Run inter-bank loan receivable streaming pipeline
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from inter_bank_loan_receivable.inter_bank_loan_receivable_streaming_pipeline import InterBankLoanReceivableStreamingPipeline


def main():
    """Run the inter-bank loan receivable pipeline"""
    print("🏦 INTER-BANK LOAN RECEIVABLE STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Uses inter-bank-loan-receivable-v4.sql query")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Currency conversion with fixing_rate table")
    print("  - ROW_NUMBER() for latest loan records")
    print("  - Complex date formatting (DDMMYYYYHHMM)")
    print("  - Past due days calculation")
    print("  - Asset classification categories")
    print("  - Batch size: 500 records per batch")
    print("=" * 60)
    
    pipeline = InterBankLoanReceivableStreamingPipeline(batch_size=500)
    
    try:
        pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ INTER-BANK LOAN RECEIVABLE STREAMING PIPELINE COMPLETED!")
        print("🔍 Key features used:")
        print("  - Full inter-bank-loan-receivable-v4.sql implementation")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Currency conversion with fixing_rate")
        print("  - ROW_NUMBER() for latest loan selection")
        print("  - Complex date formatting")
        print("  - Past due days calculation")
        print("  - Memory efficient")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Inter-bank loan receivable pipeline failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()