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
    print("INTER-BANK LOAN RECEIVABLE STREAMING PIPELINE")
    print("=" * 60)
    
    pipeline = InterBankLoanReceivableStreamingPipeline(batch_size=500)
    
    try:
        pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("INTER-BANK LOAN RECEIVABLE STREAMING PIPELINE COMPLETED!")
        print("=" * 60)

    except Exception as e:
        print(f"\nInter-bank loan receivable pipeline failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()