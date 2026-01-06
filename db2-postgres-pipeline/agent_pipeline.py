#!/usr/bin/env python3
"""
Agent Information Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.agent_processor import AgentProcessor

class AgentPipeline:
    def __init__(self, limit=None):
        """
        Agent Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get Agent table config
        self.table_config = self.config.tables.get('agents')
        if not self.table_config:
            raise ValueError("Agent table config not found")
        
        # Read the Agent query from agents-from-agents-list-NEW-V1.table.sql file
        import os
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sqls', 'agents-from-agents-list-NEW-V1.table.sql')
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            self.base_agent_query = f.read()
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.agent_processor = AgentProcessor()
        
        self.logger.info(f"👥 Agent Pipeline initialized")
        self.logger.info(f"📊 Batch limit: {self.limit}")
        self.logger.info(f"📄 Using agents-from-agents-list-NEW-V1.table.sql query")
        
    @contextmanager
    def get_db2_connection(self):
        """Get DB2 connection"""
        with self.db2_conn.get_connection() as conn:
            yield conn
            
    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            yield conn
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def clear_existing_data(self):
        """Clear existing Agent data from PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'DELETE FROM "{self.table_config.target_table}";')
                conn.commit()
                self.logger.info("🗑️ Cleared existing Agent data from PostgreSQL")
        except Exception as e:
            self.logger.error(f"❌ Failed to clear existing data: {e}")
            raise
    
    def get_total_count(self):
        """Get total count of Agent records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Count query based on the WHERE conditions in the SQL file
                count_query = """
                SELECT COUNT(*) 
                FROM AGENTS_LIST al
                RIGHT JOIN BANKEMPLOYEE be
                    ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(be.STAFF_NO)
                WHERE be.STAFF_NO IS NOT NULL
                  AND be.STAFF_NO = TRIM(be.STAFF_NO)
                  AND be.EMPL_STATUS = 1
                  AND be.STAFF_NO NOT LIKE 'ATMUSER%'
                  AND be.STAFF_NO NOT LIKE '993%'
                  AND be.STAFF_NO NOT LIKE '999%'
                  AND be.STAFF_NO NOT LIKE '900%'
                  AND be.STAFF_NO NOT LIKE 'IAP%'
                  AND be.STAFF_NO NOT LIKE 'MCB%'
                  AND be.STAFF_NO NOT LIKE 'MIP%'
                  AND be.STAFF_NO NOT LIKE 'MOB%'
                  AND be.STAFF_NO NOT LIKE 'MWL%'
                  AND be.STAFF_NO NOT LIKE 'OWP%'
                  AND be.STAFF_NO NOT LIKE 'PI0%'
                  AND be.STAFF_NO NOT LIKE 'POS%'
                  AND be.STAFF_NO NOT LIKE 'STP%'
                  AND be.STAFF_NO NOT LIKE 'TER%'
                  AND be.STAFF_NO NOT LIKE 'EIC%'
                  AND be.STAFF_NO NOT LIKE 'GEP%'
                  AND be.STAFF_NO NOT LIKE 'EYU%'
                  AND be.STAFF_NO NOT LIKE 'GLA%'
                  AND be.STAFF_NO NOT LIKE 'SYS%'
                  AND be.STAFF_NO NOT LIKE 'MLN%'
                  AND be.STAFF_NO NOT LIKE 'PET%'
                  AND be.STAFF_NO NOT LIKE 'VRT%'
                """
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def run_complete_pipeline(self):
        """Run the complete Agent pipeline"""
        self.logger.info("🚀 Starting Complete Agent Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Clear existing data
            self.logger.info("🗑️ Clearing existing Agent data...")
            self.clear_existing_data()
            
            # Step 2: Get total count
            self.logger.info("📊 Getting total Agent records...")
            total_records = self.get_total_count()
            self.logger.info(f"📊 Total Agent records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("ℹ️ No Agent records found")
                return
            
            # Step 3: Fetch all records at once (agents-from-agents-list-NEW-V1.table.sql)
            self.logger.info("📊 Fetching all Agent records...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the base query to get all records
                cursor.execute(self.base_agent_query)
                all_rows = cursor.fetchall()
                
                self.logger.info(f"👥 Fetched {len(all_rows)} agent records from agents-from-agents-list-NEW-V1.table.sql")
            
            # Show sample data
            self.logger.info("📋 Sample Agent records:")
            for i, row in enumerate(all_rows[:min(3, len(all_rows))], 1):
                self.logger.info(f"  {i}. Agent: {row[1]} | ID: {row[2]} | Status: {row[12]} | Region: {row[15]} | District: {row[16]}")
            
            # Step 4: Process records in batches
            total_processed = 0
            batch_size = 500  # Process in smaller batches for better performance
            
            for batch_start in range(0, len(all_rows), batch_size):
                batch_end = min(batch_start + batch_size, len(all_rows))
                batch_records = all_rows[batch_start:batch_end]
                batch_number = (batch_start // batch_size) + 1
                
                self.logger.info(f"\n📊 Processing batch {batch_number}: records {batch_start + 1} to {batch_end}")
                
                batch_processed = 0
                batch_skipped = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    try:
                        for row in batch_records:
                            try:
                                # Process the record using the processor
                                record = self.agent_processor.process_record(row, self.table_config.name)
                                
                                if self.agent_processor.validate_record(record):
                                    self.agent_processor.insert_to_postgres(record, pg_cursor)
                                    batch_processed += 1
                                    
                                    if batch_processed % 100 == 0:
                                        self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                                else:
                                    self.logger.warning(f"⚠️ Invalid Agent record skipped: {record.agent_id}")
                                    batch_skipped += 1
                                    
                            except Exception as e:
                                self.logger.error(f"❌ Error processing Agent record: {e}")
                                batch_skipped += 1
                                continue
                        
                        # Commit the entire batch at once
                        conn.commit()
                        self.logger.info(f"✅ Batch {batch_number} committed successfully")
                        
                    except Exception as e:
                        self.logger.error(f"❌ Batch {batch_number} failed, rolling back: {e}")
                        conn.rollback()
                        batch_processed = 0
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} new records, {batch_skipped} skipped")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
            
            # Step 5: Final verification
            self.logger.info(f"\n🎉 COMPLETE AGENT PIPELINE FINISHED!")
            self.logger.info(f"📊 Total records processed: {total_processed}")
            self.logger.info(f"📊 Expected Agent records: {total_records}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"📊 Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data
                cursor.execute(f"""
                    SELECT "agentName", "agentId", "agentStatus", "agentType", "region", "district"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "agentId" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("📋 Sample of processed records:")
                for record in sample_records:
                    agent_name, agent_id, agent_status, agent_type, region, district = record
                    self.logger.info(f"  👥 Agent: {agent_name} | ID: {agent_id} | Status: {agent_status} | Type: {agent_type} | {region}/{district}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("👥 Complete Agent Pipeline - BOT Project")
    print("=" * 60)
    
    pipeline = AgentPipeline()
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()