#!/usr/bin/env python3
"""
Pipeline Tracking System using Redis
"""

import redis
import logging
from datetime import datetime, timedelta
from typing import Optional
from config import Config

class PipelineTracker:
    """Track pipeline progress using Redis"""
    
    def __init__(self):
        self.config = Config()
        self.logger = logging.getLogger(__name__)
        
        # Connect to Redis
        try:
            self.redis_client = redis.Redis(
                host=self.config.message_queue.redis_host,
                port=self.config.message_queue.redis_port,
                db=self.config.message_queue.redis_db,
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            self.logger.info("✅ Connected to Redis for tracking")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
    
    def get_last_processed_timestamp(self, table_name: str) -> Optional[str]:
        """Get the last processed timestamp for a table"""
        try:
            key = f"pipeline:last_processed:{table_name}"
            timestamp = self.redis_client.get(key)
            
            if timestamp:
                self.logger.info(f"📅 Last processed timestamp for {table_name}: {timestamp}")
                return timestamp
            else:
                self.logger.info(f"📅 No previous timestamp found for {table_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get timestamp for {table_name}: {e}")
            return None
    
    def set_last_processed_timestamp(self, table_name: str, timestamp: str) -> bool:
        """Set the last processed timestamp for a table"""
        try:
            key = f"pipeline:last_processed:{table_name}"
            self.redis_client.set(key, timestamp)
            self.logger.info(f"✅ Updated last processed timestamp for {table_name}: {timestamp}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to set timestamp for {table_name}: {e}")
            return False
    
    def get_processing_stats(self, table_name: str) -> dict:
        """Get processing statistics for a table"""
        try:
            stats_key = f"pipeline:stats:{table_name}"
            stats = self.redis_client.hgetall(stats_key)
            
            if not stats:
                return {
                    'total_processed': 0,
                    'last_run_time': None,
                    'last_run_count': 0,
                    'errors_count': 0
                }
            
            return {
                'total_processed': int(stats.get('total_processed', 0)),
                'last_run_time': stats.get('last_run_time'),
                'last_run_count': int(stats.get('last_run_count', 0)),
                'errors_count': int(stats.get('errors_count', 0))
            }
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get stats for {table_name}: {e}")
            return {}
    
    def update_processing_stats(self, table_name: str, processed_count: int, has_error: bool = False):
        """Update processing statistics"""
        try:
            stats_key = f"pipeline:stats:{table_name}"
            current_stats = self.get_processing_stats(table_name)
            
            # Update stats
            new_total = current_stats['total_processed'] + processed_count
            error_count = current_stats['errors_count'] + (1 if has_error else 0)
            
            self.redis_client.hset(stats_key, mapping={
                'total_processed': new_total,
                'last_run_time': datetime.now().isoformat(),
                'last_run_count': processed_count,
                'errors_count': error_count
            })
            
            self.logger.info(f"📊 Updated stats for {table_name}: {processed_count} processed, {new_total} total")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to update stats for {table_name}: {e}")
    
    def reset_tracking(self, table_name: str):
        """Reset tracking for a table (for testing)"""
        try:
            timestamp_key = f"pipeline:last_processed:{table_name}"
            stats_key = f"pipeline:stats:{table_name}"
            
            self.redis_client.delete(timestamp_key)
            self.redis_client.delete(stats_key)
            
            self.logger.info(f"🔄 Reset tracking for {table_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to reset tracking for {table_name}: {e}")
    
    def get_incremental_query_filter(self, table_name: str, timestamp_column: str, 
                                   default_lookback_days: int = 7) -> str:
        """Generate WHERE clause for incremental data fetching"""
        
        last_timestamp = self.get_last_processed_timestamp(table_name)
        
        if last_timestamp:
            # Continue from last processed timestamp
            return f"AND {timestamp_column} > TIMESTAMP('{last_timestamp}')"
        else:
            # First run - get data from last N days
            return f"AND {timestamp_column} >= CURRENT_DATE - {default_lookback_days} DAYS"
    
    def show_all_tracking_info(self):
        """Show tracking information for all tables"""
        tables = ['cash_information', 'asset_owned', 'balances_bot', 'balances_with_mnos', 
                 'balance_with_other_bank', 'other_assets', 'overdraft']
        
        print("\n" + "=" * 80)
        print("📊 PIPELINE TRACKING STATUS")
        print("=" * 80)
        
        for table in tables:
            last_timestamp = self.get_last_processed_timestamp(table)
            stats = self.get_processing_stats(table)
            
            print(f"\n🔹 {table.upper()}")
            print(f"   Last Processed: {last_timestamp or 'Never'}")
            print(f"   Total Records:  {stats.get('total_processed', 0):,}")
            print(f"   Last Run:       {stats.get('last_run_time', 'Never')}")
            print(f"   Last Count:     {stats.get('last_run_count', 0):,}")
            print(f"   Errors:         {stats.get('errors_count', 0)}")
        
        print("\n" + "=" * 80)

def main():
    """Test the tracking system"""
    tracker = PipelineTracker()
    
    # Show current status
    tracker.show_all_tracking_info()
    
    # Example usage
    print("\n🧪 Testing tracking system...")
    
    # Simulate processing cash data
    tracker.set_last_processed_timestamp('cash_information', '2024-12-15 10:30:00')
    tracker.update_processing_stats('cash_information', 150)
    
    # Show updated status
    tracker.show_all_tracking_info()

if __name__ == "__main__":
    main()