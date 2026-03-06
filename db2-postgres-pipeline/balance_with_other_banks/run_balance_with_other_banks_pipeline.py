#!/usr/bin/env python3
"""
Run Balance with Other Banks Pipeline - Create table and run streaming pipeline
"""

import subprocess
import sys
import os

def run_pipeline():
    """Run the complete balance with other banks pipeline"""
    
    print("=" * 80)
    print("Balance with Other Banks Pipeline Runner")
    print("=" * 80)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("\nStep 1: Creating balanceWithOtherBanks table...")
    print("-" * 80)
    create_table_script = os.path.join(script_dir, "create_balance_with_other_banks_table.py")
    result = subprocess.run([sys.executable, create_table_script], cwd=script_dir)
    
    if result.returncode != 0:
        print("\n❌ Table creation failed!")
        sys.exit(1)
    
    print("\n✅ Table created successfully!")
    
    print("\nStep 2: Running streaming pipeline...")
    print("-" * 80)
    pipeline_script = os.path.join(script_dir, "balance_with_other_banks_streaming_pipeline.py")
    result = subprocess.run([sys.executable, pipeline_script], cwd=script_dir)
    
    if result.returncode != 0:
        print("\n❌ Pipeline failed!")
        sys.exit(1)
    
    print("\n✅ Pipeline completed successfully!")
    print("=" * 80)

if __name__ == "__main__":
    run_pipeline()
