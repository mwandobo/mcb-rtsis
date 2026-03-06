#!/usr/bin/env python3
"""
Runner script for Loans Pipeline
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
    print("LOANS PIPELINE RUNNER")
    print("="*60)
    print("\nNote: Make sure to run create_loans_table.py first if table doesn't exist")
    
    start_time = time.time()
    
    # Run streaming pipeline
    if not run_command("loans_streaming_pipeline.py", "Running loans streaming pipeline"):
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
