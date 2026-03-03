#!/usr/bin/env python3
"""
Run income statement streaming pipeline
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from income_statement.income_statement_streaming_pipeline import IncomeStatementStreamingPipeline


def main():
    """Run the income statement pipeline"""
    print("📊 INCOME STATEMENT STREAMING PIPELINE")
    print("=" * 60)
    print("📋 Features:")
    print("  - Uses income-statement.sql query")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Complex CTE aggregations with categorized amounts")
    print("  - Pattern-based account filtering")
    print("  - ECL and provision calculations")
    print("  - JSON format for list fields")
    print("  - Multiple income/expense categories")
    print("  - Batch size: 1 record per batch (single aggregated report)")
    print("=" * 60)
    
    pipeline = IncomeStatementStreamingPipeline(batch_size=1)
    
    try:
        pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ INCOME STATEMENT STREAMING PIPELINE COMPLETED!")
        print("🔍 Key features used:")
        print("  - Full income-statement.sql implementation")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Complex CTE aggregations")
        print("  - Pattern-based GL account filtering")
        print("  - JSON format for categorized amounts")
        print("  - JSONB storage for list fields")
        print("  - Memory efficient")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Income statement pipeline failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()