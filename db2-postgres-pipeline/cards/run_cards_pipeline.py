#!/usr/bin/env python3
"""
Runner script for Cards Pipeline
Creates table and runs the streaming pipeline
"""

import sys
import os
import subprocess
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_command(script_name, description):
    """Run a Python script and handle errors"""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}\n")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            check=True,
            capture_output=False
        )
        print(f"\n✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} failed with error code {e.returncode}")
        return False
    except Exception as e:
        print(f"\n✗ {description} failed: {e}")
        return False


def main():
    """Main runner function"""
    print("\n" + "="*60)
    print("CARDS PIPELINE RUNNER")
    print("="*60)
    
    start_time = time.time()
    
    # Step 1: Create table
    if not run_command("create_cards_table.py", "Step 1: Creating cardInformation table"):
        print("\n⚠ Table creation failed. Exiting...")
        sys.exit(1)
    
    print("\nWaiting 2 seconds before starting pipeline...")
    time.sleep(2)
    
    # Step 2: Run streaming pipeline
    if not run_command("cards_streaming_pipeline.py", "Step 2: Running cards streaming pipeline"):
        print("\n⚠ Pipeline execution failed")
        sys.exit(1)
    
    # Summary
    total_time = time.time() - start_time
    print("\n" + "="*60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("="*60)
    print(f"Total execution time: {total_time/60:.2f} minutes")
    print("="*60 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Pipeline failed: {e}")
        sys.exit(1)
