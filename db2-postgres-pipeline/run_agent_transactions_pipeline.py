#!/usr/bin/env python3
"""
Simple runner for Agent Transactions Pipeline
"""

import sys
import os
from datetime import datetime, timedelta

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent_transactions_pipeline import AgentTransactionsPipeline

def main():
    """Main function to run agent transactions pipeline"""
    
    print("🏪 AGENT TRANSACTIONS PIPELINE RUNNER")
    print("=" * 60)
    
    # Default parameters
    default_start_date = "2024-01-01 00:00:00"
    default_limit = 10
    
    # Check command line arguments
    start_date = default_start_date
    limit = default_limit
    
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
    if len(sys.argv) > 2:
        limit = int(sys.argv[2])
    
    print(f"📅 Start date: {start_date}")
    print(f"📊 Record limit: {limit:,}")
    print("=" * 60)
    
    try:
        # Create and run pipeline
        pipeline = AgentTransactionsPipeline(start_date, limit)
        
        print("\n🚀 Starting pipeline execution...")
        start_time = datetime.now()
        
        count = pipeline.run_complete_pipeline()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "=" * 60)
        print("✅ PIPELINE EXECUTION COMPLETED!")
        print(f"📊 Records processed: {count:,}")
        print(f"⏱️ Duration: {duration}")
        print(f"🏪 Table: agentTransactions (camelCase)")
        print("📋 Fields: reportingDate, agentId, transactionDate, transactionId, etc.")
        print("=" * 60)
        
        if count > 0:
            print("\n🔍 Next steps:")
            print("  1. Check the agentTransactions table in PostgreSQL")
            print("  2. Verify data integrity and field naming")
            print("  3. Run queries to validate the data")
            
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()