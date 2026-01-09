#!/usr/bin/env python3
import sys
sys.path.append('.')

try:
    from create_icbm_transaction_table import create_icbm_transaction_table
    print("Starting ICBM table creation...")
    create_icbm_transaction_table()
    print("ICBM table creation completed!")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()