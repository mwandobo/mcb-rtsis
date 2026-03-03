#!/usr/bin/env python3
"""
Run overdraft streaming pipeline
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from overdraft.overdraft_streaming_pipeline import OverdraftStreamingPipeline


def main():
    """Run the overdraft pipeline"""
    print("🏦 OVERDRAFT STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Uses overdraft-v3.sql query")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - CollateralAgg CTE with JSON aggregation")
    print("  - Currency conversion with fixing_rate table")
    print("  - INNER JOIN with ROW_NUMBER() for latest loan")
    print("  - Complex collateralPledged JSON structure")
    print("  - Final filtering on loanOfficer validation")
    print("  - Batch size: 1000 records per batch")
    print("=" * 60)
    
    pipeline = OverdraftStreamingPipeline(batch_size=1000)
    
    try:
        pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ OVERDRAFT STREAMING PIPELINE COMPLETED!")
        print("🔍 Key features used:")
        print("  - Full overdraft-v3.sql implementation")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - CollateralAgg CTE with JSON aggregation")
        print("  - Currency conversion with fixing_rate")
        print("  - Complex loan officer filtering")
        print("  - Memory efficient")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Overdraft pipeline failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()