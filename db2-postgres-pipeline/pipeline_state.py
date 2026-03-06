#!/usr/bin/env python3
"""
Pipeline State Management - Tracks execution state for all pipelines
"""

import psycopg2
import logging
import sys
import os
from datetime import datetime
from typing import Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PipelineStateManager:
    """Manages state for all streaming pipelines"""
    
    PIPELINES = [
        'incoming_fund_transfer',
        'outgoing_fund_transfer',
        'cards',
        'loans',
        'loan_transactions',
        'personal_data',
        'personal_data_corporates',
        'deposits',
        # Add other pipelines as they are created
    ]
    
    def __init__(self):
        self.config = Config()
        self._ensure_table_exists()
    
    def _get_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(
            host=self.config.database.pg_host,
            port=self.config.database.pg_port,
            database=self.config.database.pg_database,
            user=self.config.database.pg_user,
            password=self.config.database.pg_password
        )
    
    def _ensure_table_exists(self):
        """Create pipeline_state table if not exists"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_state (
                        pipeline_name VARCHAR(100) PRIMARY KEY,
                        last_run TIMESTAMP,
                        last_successful_run TIMESTAMP,
                        last_run_status VARCHAR(20),
                        records_processed BIGINT DEFAULT 0,
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Initialize pipelines that don't exist
                for pipeline in self.PIPELINES:
                    cursor.execute("""
                        INSERT INTO pipeline_state (pipeline_name)
                        VALUES (%s)
                        ON CONFLICT (pipeline_name) DO NOTHING
                    """, (pipeline,))
                
                conn.commit()
                logger.info("Pipeline state table ready")
        except Exception as e:
            logger.error(f"Error creating state table: {e}")
            raise
    
    def get_last_run(self, pipeline_name: str) -> Optional[datetime]:
        """Get last run timestamp for a pipeline"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT last_run FROM pipeline_state 
                    WHERE pipeline_name = %s
                """, (pipeline_name,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting last run: {e}")
            return None
    
    def get_last_successful_run(self, pipeline_name: str) -> Optional[datetime]:
        """Get last successful run timestamp"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT last_successful_run FROM pipeline_state 
                    WHERE pipeline_name = %s
                """, (pipeline_name,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting last successful run: {e}")
            return None
    
    def update_run(self, pipeline_name: str, status: str, records_processed: int = 0, 
                   error_message: str = None):
        """Update pipeline run state"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                now = datetime.now()
                
                if status == 'completed':
                    cursor.execute("""
                        UPDATE pipeline_state 
                        SET last_run = %s,
                            last_successful_run = %s,
                            last_run_status = %s,
                            records_processed = %s,
                            error_message = NULL,
                            updated_at = %s
                        WHERE pipeline_name = %s
                    """, (now, now, status, records_processed, now, pipeline_name))
                else:
                    cursor.execute("""
                        UPDATE pipeline_state 
                        SET last_run = %s,
                            last_run_status = %s,
                            records_processed = %s,
                            error_message = %s,
                            updated_at = %s
                        WHERE pipeline_name = %s
                    """, (now, status, records_processed, error_message, now, pipeline_name))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating run state: {e}")
    
    def get_all_states(self):
        """Get state for all pipelines"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pipeline_name, last_run, last_successful_run, 
                           last_run_status, records_processed, error_message
                    FROM pipeline_state
                    ORDER BY pipeline_name
                """)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting all states: {e}")
            return []
    
    def get_failed_pipelines(self):
        """Get pipelines that failed in last run"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pipeline_name, error_message 
                    FROM pipeline_state 
                    WHERE last_run_status = 'failed'
                """)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting failed pipelines: {e}")
            return []


if __name__ == "__main__":
    state_manager = PipelineStateManager()
    
    # Display all pipeline states
    print("\n" + "=" * 80)
    print("PIPELINE STATE SUMMARY")
    print("=" * 80)
    
    states = state_manager.get_all_states()
    for state in states:
        name, last_run, last_success, status, records, error = state
        print(f"\n{name}:")
        print(f"  Last Run: {last_run}")
        print(f"  Last Success: {last_success}")
        print(f"  Status: {status}")
        print(f"  Records: {records:,}")
        if error:
            print(f"  Error: {error[:100]}...")
    
    print("\n" + "=" * 80)