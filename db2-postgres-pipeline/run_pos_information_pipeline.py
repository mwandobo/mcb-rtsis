#!/usr/bin/env python3
"""
Runner for POS Information Streaming Pipeline
"""

import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pos_information_streaming_pipeline import PosInformationStreamingPipeline

def main():
    """Main function to run POS information streaming pipeline"""
    
    print("🏪 POS INFORMATION STREAMING PIPELINE RUNNER")
    print("=" * 60)
    
    # Default parameters
    default_batch_size = 10
    
    # Check command line arguments
    batch_size = default_batch_size
    
    if len(sys.argv) > 1:
        batch_size = int(sys.argv[1])
    
    print(f"📦 Batch size: {batch_size} records per batch")
    print(f"🔄 Mode: Process ALL available POS data")
    print(f"🏪 Table: posInformation (camelCase)")
    print("=" * 60)
    
    try:
        # Create and run streaming pipeline
        pipeline = PosInformationStreamingPipeline(batch_size)
        
        print("\n🚀 Starting POS streaming pipeline execution...")
        start_time = datetime.now()
        
        count = pipeline.run_streaming_pipeline()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "=" * 60)
        print("✅ POS STREAMING PIPELINE EXECUTION COMPLETED!")
        print(f"📊 Total POS records processed: {count:,}")
        print(f"📦 Batch size used: {batch_size}")
        print(f"⏱️ Duration: {duration}")
        print(f"🏪 Table: posInformation (camelCase)")
        print("📋 Fields: reportingDate, posNumber, qrFsrCode, etc.")
        print("=" * 60)
        
        if count > 0:
            print("\n🔍 Next steps:")
            print("  1. Check the posInformation table in PostgreSQL")
            print("  2. Verify all POS data has been processed")
            print("  3. Run queries to validate the complete dataset")
            
    except Exception as e:
        print(f"\n❌ POS streaming pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()