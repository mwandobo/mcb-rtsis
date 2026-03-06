#!/usr/bin/env python3
"""Runner script for Personal Data Corporates Pipeline"""
import sys, os, subprocess, time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_command(script_name, description):
    print(f"\n{'='*60}\n{description}\n{'='*60}\n")
    try:
        subprocess.run([sys.executable, script_name], cwd=os.path.dirname(os.path.abspath(__file__)), check=True, capture_output=False)
        print(f"\n✓ {description} completed")
        return True
    except Exception as e:
        print(f"\n✗ {description} failed: {e}")
        return False

def main():
    print("\n" + "="*60 + "\nPERSONAL DATA CORPORATES PIPELINE RUNNER\n" + "="*60)
    print("\nNote: Run create_personal_data_corporates_table.py first if table doesn't exist")
    start_time = time.time()
    
    if not run_command("personal_data_corporates_streaming_pipeline.py", "Running pipeline"):
        sys.exit(1)
    
    print(f"\n{'='*60}\nCOMPLETED\n{'='*60}\nTime: {(time.time()-start_time)/60:.2f} min\n{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted")
        sys.exit(1)
