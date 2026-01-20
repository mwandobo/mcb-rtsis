#!/usr/bin/env python3
"""
Runner for Batched Agent Transactions Pipeline
Processes ALL data in batches of specified size
"""

import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent_transactions_batched_pipeline import AgentTransactionsBatchedPipeline

def main():
    """Main function to run batched agent transactions pipeline"""
    
    print("🏪 BATCHED AGENT TRANSACTIONS PIPELINE RUNNER")
    print("=" * 60)
    
    # Default parameters
    default_start_date = "2024-01-01 00:00:00"
    default_batch_size = 10
    
    # Check command line arguments
    start_date = default_start_date
    batch_size = default_batch_size
    
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
    if len(sys.argv) > 2:
        batch_size = int(sys.argv[2])
    
    print(f"📅 Start date: {start_date}")
    print(f"📦 Batch size: {batch_size} records per batch")
    print(f"🔄 Mode: Process ALL available data")
    print("=" * 60)
    
    try:
        # Create and run batched pipeline
        pipeline = AgentTransactionsBatchedPipeline(start_date, batch_size)
        
        print("\n🚀 Starting batched pipeline execution...")
        start_time = datetime.now()
        
        count = pipeline.run_complete_batched_pipeline()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "=" * 60)
        print("✅ BATCHED PIPELINE EXECUTION COMPLETED!")
        print(f"📊 Total records processed: {count:,}")
        print(f"📦 Batch size used: {batch_size}")
        print(f"⏱️ Duration: {duration}")
        print(f"🏪 Table: agentTransactions (camelCase)")
        print("📋 Fields: reportingDate, agentId, transactionDate, transactionId, etc.")
        print("=" * 60)
        
        if count > 0:
            print("\n🔍 Next steps:")
            print("  1. Check the agentTransactions table in PostgreSQL")
            print("  2. Verify all data has been processed")
            print("  3. Run queries to validate the complete dataset")
            
    except Exception as e:
        print(f"\n❌ Batched pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()