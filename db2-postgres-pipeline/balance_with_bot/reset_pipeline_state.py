#!/usr/bin/env python3
"""
Reset Balance with BOT Pipeline State - Clears the last run timestamp to start from beginning
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline_state import PipelineStateManager

def reset_balance_with_bot_state():
    """Reset the balance_with_bot pipeline state to start from beginning"""
    
    print("=" * 60)
    print("Reset Balance with BOT Pipeline State")
    print("=" * 60)
    
    try:
        state_manager = PipelineStateManager()
        
        # Show current state
        print("\nCurrent state:")
        last_run = state_manager.get_last_run('balance_with_bot')
        last_success = state_manager.get_last_successful_run('balance_with_bot')
        print(f"  Last run: {last_run}")
        print(f"  Last successful run: {last_success}")
        
        # Confirm reset
        response = input("\nDo you want to reset the pipeline state? This will cause the next run to process ALL records from the beginning. (y/N): ")
        
        if response.lower() != 'y':
            print("Reset cancelled.")
            return
        
        # Reset the state
        import psycopg2
        from config import Config
        
        config = Config()
        with psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pipeline_state 
                SET last_successful_run = NULL, 
                    last_run = NULL, 
                    last_run_status = NULL,
                    records_processed = 0,
                    error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = 'balance_with_bot'
            """)
            conn.commit()
        
        print("\n✅ Pipeline state reset successfully!")
        print("The next run will process all records from the beginning.")
        print("\nTo run the pipeline:")
        print("  python balance_with_bot_streaming_pipeline.py")
        print("  or")
        print("  python run_balance_with_bot_pipeline.py")
        
    except Exception as e:
        print(f"\n❌ Error resetting pipeline state: {e}")
        sys.exit(1)

if __name__ == "__main__":
    reset_balance_with_bot_state()